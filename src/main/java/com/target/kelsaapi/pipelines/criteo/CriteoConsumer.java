package com.target.kelsaapi.pipelines.criteo;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.criteo.CriteoServiceImpl;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.criteo.CriteoRequest;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.io.IOException;

@Slf4j
public class CriteoConsumer extends EndPointConsumer implements EndPointConsumerInterface {
    private final CriteoServiceImpl SftpService;

//    private final Oauth oauth;

    public CriteoConsumer(ApplicationContext context, String pipelineRunId) throws ConfigurationException {
        super(context, pipelineRunId);
        this.SftpService = context.getBean(CriteoServiceImpl.class);
        PipelineConfig.Criteo SftpConfig = pipelineConfig.getApiconfig().getSource().getCriteo();
  //      this.oauth = new Oauth(context, SftpConfig.getAuthentication());
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

            PipelineConfig.Criteo sftpConfig = pipelineConfig.getApiconfig().getSource().getCriteo();
            CriteoRequest request = new CriteoRequest(startDate, endDate, reportType);
            CommonUtils.timerSplit(stopWatch, "Ingest from SFTP");
            String tempFile  = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.CRITEO, reportType);

            log.info("remote directory name is : " + request.getDirectory());
            log.info("remote file name is : " + request.getFileName());
            String fileName =  SftpService.getCriteoData(request, sftpConfig.hostName, sftpConfig.userName,sftpConfig.password,sftpConfig.privateKeyFile,pipelineRunId, reportType,request.getDirectory(),request.getFileName());

            //Now write to HDFS
            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,3,fileName, cleanupTempFile);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            }

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }
}
