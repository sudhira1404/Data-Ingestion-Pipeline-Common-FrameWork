package com.target.kelsaapi.pipelines.google.admanager;

import com.google.common.collect.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.service.google.admanager.forecast.GamForecastOrchestrationService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.forecast.GamLineItemForecastRequest;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class GamForecastConsumer extends EndPointConsumer {

    private final GamForecastOrchestrationService forecastService;

    public GamForecastConsumer(ApplicationContext context, String pipelineRunId) {
        super(context, pipelineRunId);

        this.forecastService = context.getBean(GamForecastOrchestrationService.class);
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
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch) throws RuntimeException {
        runPipeline(startDate, endDate, targetFile, stopWatch);
    }


    private void runPipeline(String startDate, String endDate, String targetFile, StopWatch stopWatch) throws RuntimeException {
        CommonUtils.timerSplit(stopWatch, "Initialization");
        try {
            // Initialize file handling controls
            String finalTempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.GAM, ApplicationConstants.GamReportTypes.FORECAST.name());
            List<Path> tarFiles = Lists.newArrayList();

            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;

            //Begin the download for Line Item Forecasts
            CommonUtils.timerSplit(stopWatch, "Ingest Gam Line Item Forecast Metrics from API");
            GamLineItemForecastRequest forecastRequest = new GamLineItemForecastRequest(startDate, endDate);
            List<Path> forecastFiles = forecastService.getForecasts(forecastRequest, pipelineRunId);
            tarFiles.addAll(forecastFiles);
            log.debug("Total number of temp files landed after Line Item Forecast response: {}", tarFiles.size());

            // Write to HDFS
            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            log.info("Attempting to write downloaded Gam Forecast data to HDFS...");
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,finalTempFile, tarFiles,
                    3 ,cleanupTempFile);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            } else {
                log.info("Successfully wrote the downloaded file to hdfs : " + targetFile);
            }

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }
}
