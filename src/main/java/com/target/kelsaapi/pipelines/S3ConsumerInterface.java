
package com.target.kelsaapi.pipelines;

import com.amazonaws.services.s3.AmazonS3;
import com.target.kelsaapi.common.service.listener.PipelineRunnerListener;
import com.target.kelsaapi.common.vo.s3.S3BucketParam;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.sql.SQLException;

public interface S3ConsumerInterface {

    /**
     * Executes a pipeline run.
     *
     * @param startDate The start date in yyyy-MM-dd format.
     * @param endDate The end date in yyyy-MM-dd format.
     * @param targetFile The full file path to where data should be landed.
     * @param reportType The report type to execute.
     * @param stopWatch The {@link StopWatch} instance passed from the {@link PipelineRunnerListener} to decorate.
     * @param s3BucketParam The {@link S3BucketParam} instance passed from the {@link S3BucketParam}.
     * @param s3Client The {@link AmazonS3} instance passed from the {@link AmazonS3}.
     * @throws RuntimeException A catch-all for any exception raised during the run of this pipeline.
     * @throws IOException A IOException exception raised during the run of this pipeline.
     * @throws SQLException A SQL exception raised during the run of this pipeline.
     */

    void runPipeline(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch, S3BucketParam s3BucketParam, AmazonS3 s3Client)
            throws Exception;
}
