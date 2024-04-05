package com.target.kelsaapi.common.vo.pipeline.response;

import lombok.Data;

import java.util.List;
@Data
public class PipelineRunStatusResponses {

    private final List<PipelineRunStatusResponse> pipeline_run_status_responses;

    public PipelineRunStatusResponses(List<PipelineRunStatusResponse> pipelineRunStatusResponses) {
        this.pipeline_run_status_responses = pipelineRunStatusResponses;
    }
}
