package com.target.kelsaapi.pipelines.pinterest;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.pinterest.PinterestAudienceServiceImpl;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequest;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.List;

@Slf4j
public class PinterestAudienceConsumer extends EndPointConsumer implements EndPointConsumerInterface {
    private final PinterestAudienceServiceImpl pinterestAudienceService;

    private final Oauth oauth;

    public PinterestAudienceConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.pinterestAudienceService = context.getBean(PinterestAudienceServiceImpl.class);
        PipelineConfig.Pinterest pinterestConfig = pipelineConfig.getApiconfig().getSource().getPinterest();
        this.oauth = new Oauth(context, pinterestConfig.getAuthentication());
    }

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
            //Initialize Request object
            PinterestReportRequest request = new PinterestReportRequest(startDate, endDate);

            CommonUtils.timerSplit(stopWatch, "Ingest from API");

            String tempFile  = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.PINTEREST, reportType);

            List<String> finalList =  pinterestAudienceService.getPinterestAudienceData(request, oauth, pipelineRunId, reportType);

            //Now write to HDFS
            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
            Boolean finalWriteSuccessful = writerService.writeToHDFS(finalList, targetFile,3,tempFile, cleanupTempFile);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            }

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }
}
