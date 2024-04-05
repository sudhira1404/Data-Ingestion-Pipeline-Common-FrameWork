package com.target.kelsaapi.pipelines;

import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.service.listener.PipelineRunnerListener;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.sql.SQLException;

public interface EndPointConsumerInterface {

    /**
     * Executes a pipeline run.
     *
     * @param startDate The start date in yyyy-MM-dd format.
     * @param endDate The end date in yyyy-MM-dd format.
     * @param targetFile The full file path to where data should be landed.
     * @param reportType The report type to execute.
     * @param stopWatch The {@link StopWatch} instance passed from the {@link PipelineRunnerListener} to decorate.
     * @throws RuntimeException A catch-all for any exception raised during the run of this pipeline.
     */
    void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch) throws RuntimeException, SQLException, IOException, AuthenticationException;
}
