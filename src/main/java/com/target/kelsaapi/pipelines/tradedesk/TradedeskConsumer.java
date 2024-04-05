package com.target.kelsaapi.pipelines.tradedesk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.constants.ApplicationConstants.TradedeskApiReportTypes;
import com.target.kelsaapi.common.constants.ApplicationConstants.TradedeskReportTypes;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.tradedesk.TradedeskService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class TradedeskConsumer extends EndPointConsumer implements EndPointConsumerInterface {

    private final TradedeskService tradedeskService;

    private final PipelineConfig.Tradedesk tradeDeskConfig;

    final private Oauth oauth;

    public TradedeskConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.tradedeskService = context.getBean(TradedeskService.class);
        this.tradeDeskConfig = pipelineConfig.getApiconfig().getSource().getTradedesk();
        this.oauth = new Oauth(context, tradeDeskConfig.getAuthentication());
    }

    /**
     * Executes a pipeline run.
     *
     * @param startDate  The start date in yyyy-MM-dd format.
     * @param endDate    The end date in yyyy-MM-dd format.
     * @param targetFile The full file path to where data should be landed.
     * @param reportType The report type to execute.
     * @param stopWatch  The initialized StopWatch object.
     * @throws RuntimeException A catch-all for any exception raised during the run of this pipeline.
     */
    @Override
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch)
            throws RuntimeException {
        if (reportType.contains("api")) {
            runApiPipeline(startDate, targetFile, getApiReportType(reportType), stopWatch);
        } else {
            runPipeline(startDate, targetFile, getReportType(reportType), stopWatch);
        }
    }

    private void runPipeline(String startDate, String targetFile, TradedeskReportTypes reportType,
                             StopWatch stopWatch)
            throws RuntimeException {
        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        try {
            //Initialize header
            Map<String, String> headersMap = tradedeskService.setHeaderMap(oauth);

            CommonUtils.timerSplit(stopWatch, "Ingest from API");
            log.info("working on report {}", reportType.name());
            log.debug("partnerId: {}, baseUrl: {}, contractorEndPoint: {}", tradeDeskConfig.partnerId,
                    tradeDeskConfig.baseUrl,
                    tradeDeskConfig.contractorEndPoint);

            //Fetch most recent downloadable file URL
            String reportDownloadLink = tradedeskService.getReportDownloadLink(headersMap,
                    startDate,
                    tradeDeskConfig.partnerId,
                    reportType.getReportId(),
                    tradeDeskConfig.baseUrl,
                    tradeDeskConfig.reportsEndPoint);

            //Fetch downloadable file contents
            headersMap.remove("Content-Type");
            String reportContents = tradedeskService.getReportDownload(headersMap, reportDownloadLink);
            String tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.TRADEDESK, reportType.name());
            writeToHDFS(targetFile, tempFile, stopWatch, reportContents);

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    private void runApiPipeline(String startDate, String targetFile, TradedeskApiReportTypes reportType,
                                StopWatch stopWatch)
            throws RuntimeException {
        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        try {
            //Initialize header
            Map<String, String> headersMap = tradedeskService.setHeaderMap(oauth);

            CommonUtils.timerSplit(stopWatch, "Ingest from API");
            log.info("working on report {}", reportType.name());
            log.debug("partnerId: {}, baseUrl: {}, contractorEndPoint: {}", tradeDeskConfig.partnerId,
                    tradeDeskConfig.baseUrl,
                    tradeDeskConfig.contractorEndPoint);
            ObjectMapper mapper = new ObjectMapper();
            if (reportType.name().equals("OPTIMUS_CONTRACT_FLOOR_PRICE_API")) {

                List<String> reportContents = tradedeskService.getApiDataWithPagination(
                        headersMap,
                        tradeDeskConfig.partnerId,
                        tradeDeskConfig.baseUrl,
                        tradeDeskConfig.contractorEndPoint);
                log.info("Extracted Api data from contractor: {}", tradeDeskConfig.contractorEndPoint);
                String tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.TRADEDESKAPI, reportType.name());
                String data = mapper.writeValueAsString(reportContents);
                writeToHDFS(targetFile, tempFile, stopWatch, data);

            } //else if (reportType.name().equals("OPTIMUS_ADGROUP_BID_API")) {
            else {
                List<String> advertiserIds = tradedeskService.getAllAdvertiserIds(
                        headersMap,
                        tradeDeskConfig.partnerId,
                        tradeDeskConfig.baseUrl,
                        tradeDeskConfig.advertiserEndPoint);
                log.info("count of Extracted advertisers: {}", advertiserIds.size());
                String tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.TRADEDESKAPI, reportType.name());
                int startIdx = 1;
//                List<String> adGroupData = new ArrayList<>();
                for (String advertiserId : advertiserIds) {
                    List<String> adGroupDetails = tradedeskService.getApiDataWithPagination(
                            headersMap,
                            advertiserId,
                            tradeDeskConfig.baseUrl,
                            tradeDeskConfig.adgroupAdvertiserEndPoint);
//                    adGroupData.addAll(adGroupDetails);
                    String data = mapper.writeValueAsString(adGroupDetails);
                    Boolean localWriteSuccessful = localFileWriterService.writeLocalFile(data, tempFile, true, true);
                    if (Boolean.FALSE.equals(localWriteSuccessful)) {
                        throw new IOException("write attempt to local filesystem failed for " + tempFile);
                    }
                    log.info("Extracted adgroup details data for Advertiser: {}", advertiserId );
                    log.info("completed for AdvertiserID number(index): {}", startIdx );

                    startIdx++;
                }
                log.info("tradedesk api: extracted all {} Advertisers data", advertiserIds.size());
//                String data = mapper.writeValueAsString(adGroupData);
//                writeToHDFS(targetFile, tempFile, stopWatch, data);

                CommonUtils.timerSplit(stopWatch, "Write to HDFS");
                log.info("Attempting to write downloaded ttd Adgroup bid api data to HDFS...");
                Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,3, tempFile, true);
                if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                    throw new IOException("All write attempts to HDFS failed for " + targetFile);
                }
            }
        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    private void writeToHDFS(String targetFile, String tempFile, StopWatch stopWatch, String reportContents) throws IOException {
        //Now write to HDFS
        CommonUtils.timerSplit(stopWatch, "Write to HDFS");
        Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
        Boolean finalWriteSuccessful = writerService.writeToHDFS(reportContents, targetFile, 3, tempFile, cleanupTempFile);
        if (Boolean.FALSE.equals(finalWriteSuccessful)) {
            throw new IOException("All write attempts to HDFS failed for " + targetFile);
        }
    }

    private TradedeskReportTypes getReportType(String reportType) {
        return TradedeskReportTypes.valueOf(reportType.toUpperCase());
    }

    private TradedeskApiReportTypes getApiReportType(String reportType) {
        return TradedeskApiReportTypes.valueOf(reportType.toUpperCase());
    }

}
