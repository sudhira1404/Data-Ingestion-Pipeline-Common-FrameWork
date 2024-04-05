package com.target.kelsaapi.pipelines.snapchat;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.exceptions.SnapChatException;
import com.target.kelsaapi.common.service.snapchat.SnapChatService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.ArrayList;

@Slf4j
public class SnapChatConsumer extends EndPointConsumer implements EndPointConsumerInterface {

    final private SnapChatService snapChatService;

    final private Oauth oauth;

    public SnapChatConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.snapChatService = context.getBean(SnapChatService.class);
        this.oauth = new Oauth(context, pipelineConfig.apiconfig.source.snapchat.authentication);
    }

    /**
     * Executes a pipeline run.
     *
     * @param startDate  The start date in yyyy-MM-dd format.
     * @param endDate    The end date in yyyy-MM-dd format.
     * @param targetFile The full file path to where data should be landed.
     * @param reportType The report type to execute.
     * @param stopWatch
     * @throws RuntimeException A catch-all for any exception raised during the run of this pipeline.
     */
    @Override
    @SneakyThrows
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch)
            throws RuntimeException {
        runPipeline(startDate, endDate, targetFile, reportType, stopWatch);
    }

    private void runPipeline(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch)
            throws RuntimeException, ConfigurationException {
        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        ApplicationConstants.SnapChatReportTypes level = toReportType(reportType);

        ArrayList<String> responseString;
        String tempFile= null;
        try {
            tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.SNAPCHAT, reportType);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(),e.getCause());
        }
        CommonUtils.timerSplit(stopWatch, "Ingest from API");
        try {
            switch (level) {
                case STATS:
                    responseString = snapChatService.getSnapChatCampaignStats(oauth, startDate, endDate);
                    break;
                case CAMPAIGNS:
                    responseString = snapChatService.getSnapChatCampaignDetails(oauth);
                    break;
                default:
                    throw new ConfigurationException("Requested level not currently supported: " + level);
            }
            if (responseString.size()>0) {
                CommonUtils.timerSplit(stopWatch, "Write to HDFS");
                Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
                Boolean finalWriteSuccessful = writerService.writeToHDFS(responseString, targetFile, 3, tempFile,
                        cleanupTempFile);
                if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                    throw new IOException("All write attempts to HDFS failed for " + targetFile);
                }
            } else {
                throw new SnapChatException("No data returned from Snapchat API!");
            }

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    private ApplicationConstants.SnapChatReportTypes toReportType(String reportType) throws IllegalArgumentException {
        ApplicationConstants.SnapChatReportTypes saReportType = ApplicationConstants.SnapChatReportTypes
                .valueOf(reportType.toUpperCase());
        log.info("Report type {} requested", saReportType);
        return saReportType;
    }
}