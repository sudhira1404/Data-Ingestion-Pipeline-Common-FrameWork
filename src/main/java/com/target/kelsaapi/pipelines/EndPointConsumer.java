package com.target.kelsaapi.pipelines;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.service.file.HDFSFileWriterService;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.file.XenonService;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;

@Slf4j
public abstract class EndPointConsumer implements EndPointConsumerInterface {

    protected HDFSFileWriterService writerService;

    protected XenonService xenonService;

    protected PipelineConfig pipelineConfig;

    protected LocalFileWriterService localFileWriterService;

    protected String pipelineRunId;

    public EndPointConsumer(ApplicationContext context, String pipelineRunId) {
        this.pipelineConfig = context.getBean(PipelineConfig.class);
        this.writerService = context.getBean(HDFSFileWriterService.class);
        this.xenonService = context.getBean(XenonService.class);
        this.localFileWriterService = context.getBean(LocalFileWriterService.class);
        this.pipelineRunId = pipelineRunId;
        MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME,pipelineRunId);
    }

}
