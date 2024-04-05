package com.target.kelsaapi.common.service.listener;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.exceptions.NotSupportedException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.observability.NotificationService;
import com.target.kelsaapi.common.service.postgres.pipelinerunstate.PipelineRunStateService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.SlackNotificationUtil;
import com.target.kelsaapi.common.validators.ControllerValidator;
import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import com.target.kelsaapi.common.vo.s3.S3DbParam;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import com.target.kelsaapi.pipelines.facebook.FacebookConsumer;
import com.target.kelsaapi.pipelines.google.admanager.GamActualsConsumer;
import com.target.kelsaapi.pipelines.google.admanager.GamDeliveryConsumer;
import com.target.kelsaapi.pipelines.google.admanager.GamForecastConsumer;
import com.target.kelsaapi.pipelines.google.marketingplatform.CampaignManager360Consumer;
import com.target.kelsaapi.pipelines.indexExchange.IndexExchangeConsumer;
import com.target.kelsaapi.pipelines.pinterest.PinterestAudienceConsumer;
import com.target.kelsaapi.pipelines.pinterest.PinterestConsumer;
import com.target.kelsaapi.pipelines.salesforce.SalesforceConsumer;
import com.target.kelsaapi.pipelines.criteo.CriteoConsumer;
import com.target.kelsaapi.pipelines.snapchat.SnapChatConsumer;
import com.target.kelsaapi.pipelines.switchboard.S3SwitchBoardConsumer;
import com.target.kelsaapi.pipelines.tradedesk.TradedeskConsumer;
import com.target.kelsaapi.pipelines.xander.XandrConsumer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;


import static com.target.kelsaapi.common.constants.ApplicationConstants.Sources.S3;


/**
 * JMS Listener which wakes upon messages placed on the asyncQueue. From the message, it determines which Consumer
 * class to instantiate, run, and monitor. Consumer classes are defined in {@link com.target.kelsaapi.pipelines} as
 * implementations of the {@link com.target.kelsaapi.pipelines.EndPointConsumerInterface} interface and extensions
 * of the {@link com.target.kelsaapi.pipelines.EndPointConsumer} abstract class.
 */
@Slf4j
@Service
public class PipelineRunnerListener {

    private final PipelineRunStateService pipelineRunStateService;

    private final ApplicationContext context;

    private final NotificationService notificationService;

    private final PipelineConfig.Notification notificationConfig;

    private final LocalFileWriterService localFileWriterService;

    private final boolean cleanupTempFile;

    private final ControllerValidator validator;

    /**
     * Constructor for a PipelineRunnerListener used by Spring Framework to autowire dependencies
     *
     * @param context             The {@link ApplicationContext} from Spring Framework
     * @param runState            The {@link PipelineRunStateService} from Spring Framework.
     * @param pipelineConfig      The {@link PipelineConfig} from Spring Framework.
     * @param notificationService The {@link NotificationService} from Spring Framework
     * @param validator
     */
    @Autowired
    public PipelineRunnerListener(ApplicationContext context,
                                  PipelineRunStateService runState,
                                  PipelineConfig pipelineConfig,
                                  NotificationService notificationService,
                                  LocalFileWriterService localFileWriterService, ControllerValidator validator) {
        this.pipelineRunStateService = runState;
        this.context = context;
        this.notificationConfig = pipelineConfig.getApiconfig().getNotification();
        this.notificationService = notificationService;
        this.localFileWriterService = localFileWriterService;
        this.cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
        this.validator = validator;
    }

    /**
     * Main JMS Listener method that wires to the asyncQueue and gets triggered by new messages published to the queue.
     *
     * @param runState An abstract {@link PipelineRunState} class that will further be determined the concrete implementation class.
     */
    @Async(value = "pipelineRunnerListenerExecutor")
    public void runPipeline(PipelineRunState runState) throws ParseException, ConfigurationException {

        String pipelineRunId = runState.getBatchRequestId();
        MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME, pipelineRunId);
        StopWatch stopWatch = initializeStopwatch(pipelineRunId);
        String startDate = runState.getStartDate();
        String endDate = runState.getEndDate();
        String targetFile = runState.getLandingFile();
        String reportType = runState.getSourceReportType();
        String source = runState.getSourceSystem();

        //Adding date format validators here to prevent these from flowing downstream to the S3Consumer who may potentially use SQL to look these up in a sql injection type of attack
        if (!validator.validateDateFormats(startDate, endDate)) {
            cleanup(runState, stopWatch, true, source, startDate, endDate, targetFile, reportType, pipelineRunId);
            throw new ConfigurationException("Invalid date formats detected in pipeline request");
        } else {
            Thread thisThread = Thread.currentThread();

            Runtime runtime = Runtime.getRuntime();
            Thread updatePipeline = new Thread(() -> shutdownThread(runState, stopWatch, source, startDate, endDate, targetFile, reportType, pipelineRunId, thisThread));
            runtime.addShutdownHook(updatePipeline);

            log.info("Will use Source and ReportType to determine the correct consumer to initialize");
            runPipeline(runState, pipelineRunId, source, startDate, endDate, targetFile, stopWatch, reportType);
            runtime.removeShutdownHook(updatePipeline);
        }
    }

    /**
     * A wrapper method for sending notifications if configured.
     *
     * @param exceptionsThrown   True if exceptions were thrown (and failure state detected). False otherwise.
     * @param stopwatch          The {@link StopWatch} instance
     * @param sourceSystem       The name of the source system. This either came from the {@link ApplicationConstants.Sources}
     *                           or from the {@link ApplicationConstants.Sources} enums, depending on whether the pipeline was
     *                           v1 or v2.
     * @param startDate          The start date in yyyy-MM-dd format.
     * @param endDate            The end date in yyyy-MM-dd format.
     * @param landingFile        The file and path where data will be landed.
     * @param batchRequestStatus The final status of the pipeline.
     * @param reportType         The name of the report type for the specified sourceSystem. For v2 pipelines.
     */
    private void sendNotification(Boolean exceptionsThrown, StopWatch stopwatch,
                                  String sourceSystem, String startDate, String endDate, String landingFile,
                                  String batchRequestStatus, @Nullable String reportType) {

        ApplicationConstants.slackMessageType slackMessageType;
        if (!exceptionsThrown) {
            slackMessageType = ApplicationConstants.slackMessageType.SUCCESS;
        } else {
            slackMessageType = ApplicationConstants.slackMessageType.FAILURE;
        }
        log.info("Printing slackMessageType:" + slackMessageType);
        if (SlackNotificationUtil.isSlackEnabled(notificationConfig)) {
            if (reportType == null) {
                log.warn("DEPRECATION WARNING: Notification missing ReportType field. Please migrate to v2 endpoint");
            }
            notificationService.sendPipelineNotification(slackMessageType, stopwatch, sourceSystem,
                    startDate,
                    endDate,
                    landingFile,
                    batchRequestStatus,
                    reportType);
        }
    }

    /**
     * Initializes a {@link StopWatch} instance for a new pipeline run
     *
     * @param pipelineRunId The ID of the Pipeline Run to associate with this StopWatch Instance
     * @return A new {@link StopWatch} instance tied to the given pipelineRunId
     */
    private StopWatch initializeStopwatch(String pipelineRunId) {
        log.info("Running background process now for Pipeline run ID: " + pipelineRunId);
        //Starts StopWatch
        StopWatch stopWatch = new StopWatch(pipelineRunId);
        CommonUtils.timerSplit(stopWatch, "Update Pipeline status to RUNNING in db");
        return stopWatch;
    }

    private void runSwitchboardConsumer(String sourceSystem, String reportType, String startDate,
                                        String endDate, String targetFile, StopWatch stopWatch, String pipelineRunId)
            throws SQLException, IOException, AuthenticationException {
        String concatSourceAndReportType = sourceSystem.toUpperCase() + reportType.toUpperCase();
        runSwitchboardConsumer(concatSourceAndReportType,startDate,endDate, targetFile, stopWatch, pipelineRunId);
    }

    private void runSwitchboardConsumer(String concatSourceAndReportType, String startDate,
                                        String endDate, String targetFile, StopWatch stopWatch, String pipelineRunId)
            throws SQLException, IOException, AuthenticationException {
        log.info("concatSourceAndReportType:" + concatSourceAndReportType);
        S3SwitchBoardConsumer s3SwitchBoardConsumer = new S3SwitchBoardConsumer(context, pipelineRunId);
        s3SwitchBoardConsumer.execute(startDate, endDate, targetFile, concatSourceAndReportType, stopWatch);
    }

    /**
     * Helper class for the main {@link #runPipeline(PipelineRunState)} method called when a request is made.
     *
     * @param runState      The {@link PipelineRunState} class of this run.
     * @param pipelineRunId The Pipeline Run Id
     * @param sourceName    The Source system name
     * @param startDate     The start date
     * @param endDate       The end date
     * @param targetFile    The targetFile
     * @param stopWatch     The {@link StopWatch} instance as initialized first via {@link #initializeStopwatch(String)}
     * @param reportType    The report type as determined by the source system
     */
    private void runPipeline(PipelineRunState runState,
                             String pipelineRunId, String sourceName,
                             String startDate, String endDate, String targetFile, StopWatch stopWatch,
                             String reportType) {
        boolean exceptionsThrown = false;
        ApplicationConstants.Sources sourceSystem;
        try {
            PipelineRunState runStateFromDb = pipelineRunStateService.findById(pipelineRunId);

            runState.setbatchRequestStatus(ApplicationConstants.PipelineStates.RUNNING);
            runState.setCreatedTimestamp(runStateFromDb.getCreatedTimestamp());
            pipelineRunStateService.update(runState);

            if (S3DbParam.getReportType().isEmpty()) {
                sourceSystem = ApplicationConstants.Sources.valueOf(sourceName.toUpperCase());
            }
            else {
                sourceSystem = S3;
            };
            pipelineResolver(sourceSystem, reportType, startDate, endDate, targetFile, stopWatch, pipelineRunId,sourceName);

            runState.setbatchRequestStatus(ApplicationConstants.PipelineStates.COMPLETED);
        } catch (Exception e) {
            exceptionsThrown = true;
            runState.setbatchRequestStatus(ApplicationConstants.PipelineStates.FAILED);
            log.error(e.getMessage(), e.getCause());
        } finally {
            cleanup(runState, stopWatch, exceptionsThrown, sourceName, startDate, endDate, targetFile, reportType, pipelineRunId);
        }
    }

    private void pipelineResolver(ApplicationConstants.Sources sourceSystem, String reportType, String startDate,
                                  String endDate, String targetFile, StopWatch stopWatch, String pipelineRunId, String sourceName)
            throws NotSupportedException, ConfigurationException, IOException, SQLException, AuthenticationException {

        if (S3DbParam.getReportType().isEmpty()) {
            switch (sourceSystem) {
                case FACEBOOK -> {
                    ApplicationConstants.FacebookReportTypes rt = ApplicationConstants.FacebookReportTypes.valueOf(reportType.toUpperCase());
                    if (rt == ApplicationConstants.FacebookReportTypes.CAMPAIGN_INSIGHTS) {
                        FacebookConsumer facebookConsumerService = new FacebookConsumer(context, pipelineRunId);
                        facebookConsumerService.execute(startDate, endDate, targetFile, reportType, stopWatch);
                    } else {
                        throw new NotSupportedException("Requested report type for Facebook is not currently supported: " + rt);
                    }
                }
                case GAM -> {
                    ApplicationConstants.GamReportTypes grt = ApplicationConstants.GamReportTypes.valueOf(reportType.toUpperCase());
                    switch (grt) {
                        case ACTUALS -> {
                            GamActualsConsumer gamActualsConsumerService = new GamActualsConsumer(context, pipelineRunId);
                            gamActualsConsumerService.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        case DELIVERY -> {
                            GamDeliveryConsumer gamDeliveryConsumer = new GamDeliveryConsumer(context, pipelineRunId);
                            gamDeliveryConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        case FORECAST -> {
                            GamForecastConsumer gamForecastConsumer = new GamForecastConsumer(context, pipelineRunId);
                            gamForecastConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        default ->
                                throw new NotSupportedException("Requested report type for Google Ad Manager is not currently supported: " + grt);
                    }
                }
                case TRADEDESK -> {
                    ApplicationConstants.TradedeskReportTypes ttrt = ApplicationConstants.TradedeskReportTypes.valueOf(reportType.toUpperCase());
                    switch (ttrt) {
                        case ACTUALS, OPTIMUS_AVAILABILITY, OPTIMUS_PERFORMANCE, OPTIMUS_ADGROUP_CONTRACT -> {
                            TradedeskConsumer tradedeskConsumerService = new TradedeskConsumer(context, pipelineRunId);
                            tradedeskConsumerService.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        default ->
                                throw new NotSupportedException("Requested report type for Tradedesk is not currently supported: " + ttrt);
                    }
                }
                case TRADEDESKAPI -> {
                    ApplicationConstants.TradedeskApiReportTypes ttrt = ApplicationConstants.TradedeskApiReportTypes.valueOf(reportType.toUpperCase());
                    switch (ttrt) {
                        case OPTIMUS_CONTRACT_FLOOR_PRICE_API, OPTIMUS_ADGROUP_BID_API -> {
                            TradedeskConsumer tradedeskConsumerService = new TradedeskConsumer(context, pipelineRunId);
                            tradedeskConsumerService.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        default ->
                                throw new NotSupportedException("Requested report type for Tradedesk is not currently supported: " + ttrt);
                    }
                }
                case SNAPCHAT -> {
                    ApplicationConstants.SnapChatReportTypes srt = ApplicationConstants.SnapChatReportTypes.valueOf(reportType.toUpperCase());
                    switch (srt) {
                        case STATS:
                        case CAMPAIGNS:
                            SnapChatConsumer snapChatConsumerStats = new SnapChatConsumer(context, pipelineRunId);
                            snapChatConsumerStats.execute(startDate, endDate, targetFile, reportType, stopWatch);
                            break;
                        default:
                            throw new NotSupportedException("Requested report type for Snapchat is not currently supported: " + srt);
                    }
                }
                case PINTEREST -> {
                    ApplicationConstants.PinterestReportTypes prt = ApplicationConstants.PinterestReportTypes.valueOf(reportType.toUpperCase());
                    switch (prt) {
                        case CAMPAIGN -> {
                            PinterestConsumer pinterestConsumer = new PinterestConsumer(context, pipelineRunId);
                            pinterestConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        case AUDIENCE -> {
                            PinterestAudienceConsumer pinterestAudienceConsumer = new PinterestAudienceConsumer(context, pipelineRunId);
                            pinterestAudienceConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                        }
                        default ->
                                throw new NotSupportedException("Requested report type for Pinterest is not currently supported: " + prt);

                    }
                }
                case CAMPAIGN_MANAGER_360 -> {
                    ApplicationConstants.CampaignManager360ReportTypes cmrt = ApplicationConstants.CampaignManager360ReportTypes.valueOf(reportType.toUpperCase());
                    if (cmrt == ApplicationConstants.CampaignManager360ReportTypes.CAMPAIGN) {
                        CampaignManager360Consumer campaignManager360Consumer = new CampaignManager360Consumer(context, pipelineRunId);
                        campaignManager360Consumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                    } else {
                        throw new NotSupportedException("Requested report type for Campaign Manager 360 is not currently supported: " + cmrt);
                    }
                }

                case XANDR -> {
                    ApplicationConstants.XandrReportTypes xrt = ApplicationConstants.XandrReportTypes.valueOf(reportType.toUpperCase());
                    List<ApplicationConstants.XandrReportTypes> xandrReportTypes = Arrays.asList(ApplicationConstants.XandrReportTypes.values());
//                    String[] reportTypes = Stream.of(ApplicationConstants.XandrReportTypes.values()).map(Enum::name).toArray(String[]::new);
                    if (xandrReportTypes.contains(xrt)) {
                        XandrConsumer xandrConsumer = new XandrConsumer(context, pipelineRunId);
                        xandrConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                    } else {
                        throw new NotSupportedException("Requested report type for Xandr is not currently supported: " + xrt);
                    }
                }

                case INDEXEXCHANGE -> {
                    ApplicationConstants.IndexExchangeReportTypes iert = ApplicationConstants.IndexExchangeReportTypes.valueOf(reportType.toUpperCase());
                    if (iert == ApplicationConstants.IndexExchangeReportTypes.DEALMETRICS) {
                        IndexExchangeConsumer indexExchangeConsumer = new IndexExchangeConsumer(context, pipelineRunId);
                        indexExchangeConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                    } else {
                        throw new NotSupportedException("Requested report type for Campaign Manager 360 is not currently supported: " + iert);
                    }
                }

                case SALESFORCE -> {
                    ApplicationConstants.SalesforceReportTypes srt = ApplicationConstants.SalesforceReportTypes.valueOf(reportType.toUpperCase());
                    switch (srt) {
                        case ACCOUNT:
                        case BRAND__C:
                        case OPPORTUNITY:
                        case BRAND_AGENCY_ASSIGNMENT__C:
                        case BRAND_OPPORTUNITY__C:
                        case CAMPAIGN__C:
                        case CAMPAIGN_PRODUCT__C:
                        case CLASS__C:
                        case CONTACT:
                        case REVENUE__C:
                        case USER:
                        case VENDOR_NUMBER__C:
                            SalesforceConsumer salesforceConsumer = new SalesforceConsumer(context, pipelineRunId);
                            salesforceConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                            break;
                        default:
                            throw new NotSupportedException("Requested report type for Salesforce is not currently supported: " + srt);

                    }
                }

                case CRITEO -> {
                    ApplicationConstants.SftpReportTypes sftp = ApplicationConstants.SftpReportTypes.valueOf(reportType.toUpperCase());
                    switch (sftp) {
                        case CRITEO_CAMPAIGN_DLY_SNAPSHOT:
                        case CRITEO_KEYWORD_DLY_SNAPSHOT:
//                    if (sftp == ApplicationConstants.SftpReportTypes.CRITEO_CAMPAIGN_DLY_SNAPSHOT) {
                            CriteoConsumer sftpConsumer = new CriteoConsumer(context, pipelineRunId);
                            sftpConsumer.execute(startDate, endDate, targetFile, reportType, stopWatch);
                            break;
//                    } else {
                        default:
                            throw new NotSupportedException("Requested report type for sftp is not currently supported: " + sftp);
                    }
                }
            }
        }
        else {

            if ((sourceName.toUpperCase() + reportType.toUpperCase()).equals(S3DbParam.getReportType().toUpperCase()))
                runSwitchboardConsumer(sourceName, reportType, startDate, endDate, targetFile, stopWatch, pipelineRunId);
            else {

                throw new ConfigurationException("Requested source not currently supported:=>" + sourceSystem +
                        ".Need to have parameter entry in PGDB(mdf_s3_parameter) with source_report_type value as:" +
                        sourceName.toUpperCase() + reportType.toUpperCase() +
                        " and actv_f=Y to support S3 ingestion reading parameter from database" +
                        " else add parameters in application constants");

            }
        }
    }
    private void cleanup(PipelineRunState runState, StopWatch stopWatch, Boolean exceptionsThrown, String sourceName,
                         String startDate, String endDate, String targetFile, String reportType, String pipelineRunId) {
        CommonUtils.timerSplit(stopWatch, "Update Pipeline status to " + runState.getBatchRequestStatus() + " in db");
        try {
            pipelineRunStateService.update(runState);
        } catch (Exception e) {
            exceptionsThrown = true;
            log.error("Unable to update final status of pipeline run state in db for {}", pipelineRunId);
            log.error(e.getMessage(), e.getCause());
        } finally {
            CommonUtils.timerSplit(stopWatch, "Send notification");
            sendNotification(exceptionsThrown, stopWatch, sourceName, startDate, endDate, targetFile,
                    runState.getBatchRequestStatus(), reportType);
            stopWatch.stop();

            if (cleanupTempFile) {
                log.info("Looking for any temp files to cleanup in the local filesystem.");
                try {
                    localFileWriterService.deleteLocalFiles(localFileWriterService.findFilesFromNamePrefix(pipelineRunId));
                } catch (IOException e) {
                    log.error("Unable to cleanup temp files");
                    log.error(e.getMessage(), e.getCause());
                }
            }

            log.info("Completed pipeline run");
            log.info("Final status: {}", runState.getBatchRequestStatus());
            log.info(CommonUtils.prettyPrintStopWatchSeconds(stopWatch));
        }
    }
    private void shutdownThread(PipelineRunState runState, StopWatch stopWatch, String sourceName, String startDate, String endDate, String targetFile, String reportType, String pipelineRunId, Thread id) {

        runState.setbatchRequestStatus(ApplicationConstants.PipelineStates.FAILED);
        cleanup(runState, stopWatch, true, sourceName, startDate, endDate, targetFile, reportType, pipelineRunId);
        if (id.isAlive()) {
            log.warn("Attempting to shut down thread {}", id.getName());
            id.interrupt();
        }
    }
}
