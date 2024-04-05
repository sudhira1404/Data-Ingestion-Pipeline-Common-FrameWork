package com.target.kelsaapi.pipelines.facebook;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.service.facebook.FacebookService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.facebook.FacebookAdsInsightsRequest;
import com.target.kelsaapi.common.vo.facebook.FacebookAdsInsightsResponse;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.util.List;

@Slf4j
public class FacebookConsumer extends EndPointConsumer {

    private final FacebookService facebookService;

    public FacebookConsumer(ApplicationContext context, String pipelineRunId) {
        super(context, pipelineRunId);
        this.facebookService = context.getBean(FacebookService.class);
    }

    @Override
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch) throws RuntimeException {
        runPipeline(startDate, endDate, targetFile, reportType, stopWatch);
    }

    private void runPipeline(String startDate, String endDate, String targetFile, @Nullable String reportType, StopWatch stopWatch) {
        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        try {
            //Now initialize the Final AdsInsights Request object
            FacebookAdsInsightsRequest adsInsightsRequest = new FacebookAdsInsightsRequest();
            adsInsightsRequest.init();
            adsInsightsRequest.setTimeRangeFormatted(startDate, endDate);

            CommonUtils.timerSplit(stopWatch, "Ingest from API");
            //Get Ads Insights
            FacebookAdsInsightsResponse adsInsightsResponse = facebookService.getApiData(adsInsightsRequest);
            List<String> adsInsights = adsInsightsResponse.getAdsInsightsReportResults();
            //Final list to write to a file
            log.info("Total number of Ads Insights collected across all Ad Accounts: " + adsInsights.size());

            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            //Now let's write to hdfs
            String tempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.FACEBOOK, reportType);
            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;

            log.debug("Target file:" + targetFile);
            log.info("Attempting to write Ads Insights to HDFS...");
            Boolean successFileWrite = writerService.writeToHDFS(adsInsights, targetFile, 3, tempFile, cleanupTempFile);

            if (successFileWrite) {
                log.info("Successful writing final Insights file to HDFS");
            } else {
                log.error("Failed writing Insights to file");
                throw new RuntimeException("Failed writing Insights to file");
            }

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());

        }
    }


}