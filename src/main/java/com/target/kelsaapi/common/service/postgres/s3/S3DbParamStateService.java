package com.target.kelsaapi.common.service.postgres.s3;

import com.target.kelsaapi.common.vo.s3.S3DbParamState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

@Repository
@Service
@Slf4j
public class S3DbParamStateService {

    private final S3DbParamStateRepository repository;

    @Autowired
    public S3DbParamStateService(S3DbParamStateRepository repository) {
        this.repository = repository;
    }

    public S3DbParamState getS3DbParamState(String sourceReportType) {
        return repository.getS3DbParamState(sourceReportType);
    }

}
