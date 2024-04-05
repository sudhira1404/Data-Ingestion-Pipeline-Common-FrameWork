package com.target.kelsaapi.pipelines.switchboard;

import com.amazonaws.services.s3.AmazonS3;
import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.service.s3.auth.S3AuthenticationService;
import com.target.kelsaapi.common.vo.s3.S3BucketParam;
import com.target.kelsaapi.pipelines.S3Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class S3SwitchBoardConsumer extends S3Consumer  {

    protected S3AuthenticationService s3AuthenticationService;
    String profileName = pipelineConfig.apiconfig.source.getS3SwitchBoard().getProfileName();
    String bucketName = pipelineConfig.apiconfig.source.getS3SwitchBoard().getBucketName();
    String regionName = pipelineConfig.apiconfig.source.getS3SwitchBoard().getRegionName();
    String credentialsFileLocation = pipelineConfig.apiconfig.source.getS3SwitchBoard().getCredentialsFileLocation();


    S3BucketParam s3BucketParam  = new S3BucketParam(profileName,bucketName,regionName);

    public S3SwitchBoardConsumer(ApplicationContext context, String pipelineRunId) throws AuthenticationException {
        super(context, pipelineRunId);
        this.s3AuthenticationService = context.getBean(S3AuthenticationService.class);
    }

    @Override
    @SneakyThrows
        public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch) throws AuthenticationException,RuntimeException, SQLException, IOException {
        log.info("Calling the s3AuthenticationService with parameters " + s3BucketParam.toString());
        AmazonS3 s3Client = s3AuthenticationService.s3Client(profileName, regionName, bucketName, credentialsFileLocation);
            log.info("Calling the s3 consumer");
            runPipeline(startDate, endDate, targetFile, reportType, stopWatch,s3BucketParam, s3Client);
        }
}
