package com.target.kelsaapi.common.service.postgres.pipelinerunstate;

import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository("pipelineRunStateRepository")
public interface PipelineRunStateRepository  extends CrudRepository<PipelineRunState, String> {

}
