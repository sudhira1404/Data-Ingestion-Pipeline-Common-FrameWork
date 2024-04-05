package com.target.kelsaapi.pipelines.salesforce;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.salesforce.SalesforceServiceImpl;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.Oauth;
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
public class SalesforceConsumer extends EndPointConsumer implements EndPointConsumerInterface {
    private final SalesforceServiceImpl salesforceService;

    private final Oauth oauth;

    public SalesforceConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.salesforceService = context.getBean(SalesforceServiceImpl.class);
        PipelineConfig.Salesforce salesforceConfig = pipelineConfig.getApiconfig().getSource().getSalesforce();
        this.oauth = new Oauth(context, salesforceConfig.getAuthentication());
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

            CommonUtils.timerSplit(stopWatch, "Ingest from API");

            String tempFile  = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.SALESFORCE, reportType);

            List<String> finalList =  salesforceService.getSalesforceObjectData(oauth, pipelineRunId, reportType);

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
