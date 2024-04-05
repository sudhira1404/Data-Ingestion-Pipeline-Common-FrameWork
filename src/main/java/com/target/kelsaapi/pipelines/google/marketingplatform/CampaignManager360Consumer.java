package com.target.kelsaapi.pipelines.google.marketingplatform;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.service.google.marketingplatform.CampaignManager360Interface;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.google.request.marketingplatform.CampaignManager360ReportRequest;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;

/**
 * Main pipeline consumer class for Campaign Manager 360
 */
@Slf4j
public class CampaignManager360Consumer extends EndPointConsumer {

    private final CampaignManager360Interface campaignManager360Service;

    /**
     * Constructor to be used when instantiating this class
     *
     * @param context The {@link ApplicationContext} from Spring Framework
     */
    public CampaignManager360Consumer(ApplicationContext context, String pipelineRunId) {
        super(context, pipelineRunId);
        this.campaignManager360Service = context.getBean(CampaignManager360Interface.class);
    }

    /**
     * Executes a pipeline run.
     *
     * @param startDate  The start date in yyyy-MM-dd format.
     * @param endDate    The end date in yyyy-MM-dd format.
     * @param targetFile The full file path to where data should be landed.
     * @param reportType The report type to execute.
     * @param stopWatch A {@link StopWatch} instance used to time various steps of the pipeline execution.
     * @throws RuntimeException A catch-all for any exception raised during the run of this pipeline.
     */
    @Override
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch)
            throws RuntimeException {
        runPipeline(startDate, endDate, targetFile, reportType, stopWatch);
    }

    private void runPipeline(String startDate, String endDate, String targetFile, @Nullable String reportType,
                             StopWatch stopWatch) throws RuntimeException {

        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        try {
            PipelineConfig.Google.MarketingPlatform config = pipelineConfig.apiconfig.source.google.marketingPlatform;
            log.debug("CampaignManager360Interface json key file path: " + config.jsonKeyFilePath);
            String tempFile;
            tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.CAMPAIGN_MANAGER_360, reportType);
            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;

            //Initialize Report Request object
            CampaignManager360ReportRequest request = new CampaignManager360ReportRequest(startDate, endDate);

            CommonUtils.timerSplit(stopWatch, "Ingest from API");
            log.info("Attempting to download Campaign Manager 360 data from API");
            //Call method that is used to generate query object and load data to a local path.
            InputStream campaignManagerData = campaignManager360Service.getData(request);
            Boolean localWriteSuccessful = localFileWriterService.retryableLocalWrite(campaignManagerData, tempFile);
            if (Boolean.FALSE.equals(localWriteSuccessful)) {
                throw new IOException("All write attempts to local filesystem failed for " + tempFile);
            }
            log.info("Campaign Manager 360 data successfully downloaded from API!");

            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            log.info("Attempting to write downloaded Gam data to HDFS...");
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,3, tempFile, cleanupTempFile);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            }


        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }
}
