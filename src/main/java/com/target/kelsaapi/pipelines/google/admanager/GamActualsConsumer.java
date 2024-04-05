package com.target.kelsaapi.pipelines.google.admanager;

import com.google.api.client.auth.oauth2.Credential;
import com.google.common.collect.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.google.admanager.GamAuthenticationService;
import com.target.kelsaapi.common.service.google.admanager.actuals.GamActualsService;
import com.target.kelsaapi.common.service.google.admanager.actuals.GamGeoService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.actuals.GamActualsRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.actuals.GamActualsResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.actuals.GamGeoResponse;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GamActualsConsumer extends EndPointConsumer implements EndPointConsumerInterface {

    private final GamActualsService gamActuals;

    private final GamGeoService gamGeo;

    private final GamAuthenticationService auth;

    public GamActualsConsumer(ApplicationContext context, String pipelineRunId) {
        super(context, pipelineRunId);
        this.gamActuals = context.getBean(GamActualsService.class);
        this.gamGeo = context.getBean(GamGeoService.class);
        this.auth = context.getBean(GamAuthenticationService.class);
    }

    @Override
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch) throws RuntimeException {
        runPipeline(startDate, endDate, targetFile, reportType, stopWatch);
    }

    private void runPipeline(String startDate, String endDate, String targetFile, @Nullable String reportType, StopWatch stopWatch) {
        
        int GEO_PARTITION_LIST_SIZE = 1000;

        int WRITE_ATTEMPTS = 3;

        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");
        try {
            String finalTempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.GAM, ApplicationConstants.GamReportTypes.ACTUALS.name());
            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
            CommonUtils.timerSplit(stopWatch, "Ingest from API and write to local file");
            log.info("Attempting to download Gam data from API");
            List<Path> tempFilesPath = new ArrayList<>();
            int fileInc =1;
            Credential oAuth2Credential = auth.get();

            String tempFileRoot = CommonUtils.generateTempFileRootPath() + pipelineRunId + "_report-" + startDate;
            GamGeoResponse gamGeoData = (GamGeoResponse) gamGeo.get(oAuth2Credential);
            for (List<String> zipCodeList : Lists.partition(gamGeoData.getResponseList(), GEO_PARTITION_LIST_SIZE))
            {
                String zipCode = StringUtils.join(zipCodeList, ',');
                String tempFile = tempFileRoot + "_" + fileInc + ".csv";
                tempFilesPath.add(Paths.get(tempFile));
                fileInc++;
                log.info("Gam data successfully downloaded from API!");
                GamActualsRequest request = new GamActualsRequest(startDate, endDate, zipCode);
                GamActualsResponse actuals = (GamActualsResponse) gamActuals.get(request, oAuth2Credential);
                localFileWriterService.writeLocalFile(actuals.getResponseList(),tempFile, false);
            }

            if (tempFilesPath.isEmpty()) throw new GamException("No files downloaded!");
            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            log.info("Attempting to write downloaded Gam data to HDFS...");
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,finalTempFile,tempFilesPath,
                    WRITE_ATTEMPTS,cleanupTempFile);
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

