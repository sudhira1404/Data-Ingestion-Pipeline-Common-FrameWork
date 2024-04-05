package com.target.kelsaapi.common.vo.pipeline.response;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.target.kelsaapi.common.vo.pipeline.request.PipelineRunRequest;
import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import lombok.Data;
import lombok.Getter;

@Data
public class PipelineRunStatusResponse {

    @JsonProperty("pipeline_run_id")
    @Getter(onMethod_ = {@JsonGetter(value = "pipeline_run_id")})
    protected String pipelineRunId;

    @JsonProperty("pipeline_run_status")
    @Getter(onMethod_ = {@JsonGetter(value = "pipeline_run_status")})
    protected String pipelineRunStatus;

    @JsonProperty("pipeline_run_request")
    @Getter(onMethod_ = {@JsonGetter(value = "pipeline_run_request")})
    private PipelineRunRequest pipelineRunRequest;

    public PipelineRunStatusResponse(PipelineRunState pipelineRunState) {
        this.pipelineRunId = pipelineRunState.getBatchRequestId();
        this.pipelineRunStatus = pipelineRunState.getBatchRequestStatus();
        this.pipelineRunRequest = new PipelineRunRequest(pipelineRunState);
    }

}
