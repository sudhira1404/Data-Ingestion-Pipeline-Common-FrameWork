package com.target.kelsaapi.common.service.postgres.pipelinerunstate;

import com.target.kelsaapi.common.exceptions.BadResourceException;
import com.target.kelsaapi.common.exceptions.ResourceAlreadyExistsException;
import com.target.kelsaapi.common.exceptions.ResourceNotFoundException;
import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

@Repository
@Service
@Slf4j
public class PipelineRunStateService {

    @Autowired
    private PipelineRunStateRepository repository;

    private boolean existsById(String id) {
        return repository.existsById(id);
    }

    public PipelineRunState findById(String id) throws ResourceNotFoundException {
        PipelineRunState pipelineState = repository.findById(id).orElse(null);
        if (pipelineState == null){
            throw new ResourceNotFoundException("Cannot find Pipeline with id: " + id);
        } else {
            return pipelineState;
        }
    }

    @Transactional
    public void save(PipelineRunState pipelineRunState) throws BadResourceException, ResourceAlreadyExistsException {
        if (!ObjectUtils.isEmpty(pipelineRunState.getSourceSystem())) {
            if (pipelineRunState.getBatchRequestId() != null && existsById(pipelineRunState.getBatchRequestId())) {
                throw new ResourceAlreadyExistsException("Pipeline requested already exists: " + pipelineRunState.getBatchRequestId());
            }
            repository.save(pipelineRunState);
        } else {
            BadResourceException exc = new BadResourceException("Failed to save PipelineRunState");
            exc.addErrorMessage("Pipeline is empty or null");
            throw exc;
        }
    }

    @Transactional
    public void update(PipelineRunState pipelineRunState) throws BadResourceException, ResourceNotFoundException {
        if (!ObjectUtils.isEmpty(pipelineRunState.getBatchRequestId())) {
            String batchRequestId = pipelineRunState.getBatchRequestId();
            log.debug("Pipeline run id to update in the db: " + batchRequestId);
            boolean exists;
            try {
                exists = repository.existsById(pipelineRunState.getBatchRequestId());
            } catch (NullPointerException e) {
                throw new ResourceNotFoundException(e.getMessage());
            }
            if (!exists) {
                throw new ResourceNotFoundException("Cannot find Pipeline Run Id with id: " + batchRequestId);
            }
            repository.save(pipelineRunState);
        }
        else {
            BadResourceException exc = new BadResourceException("Failed to save PipelineRunState");
            exc.addErrorMessage("Pipeline is null or empty");
            throw exc;
        }
    }

}
