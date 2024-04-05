package com.target.kelsaapi.common.service.postgres.s3;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.target.kelsaapi.common.util.textFormatterInterface;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryState;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryStateId;
import jakarta.persistence.EntityManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Stream;


@Repository
@Service
@Slf4j
public class S3ObjectSummaryStateService implements textFormatterInterface {

    private final S3ObjectSummaryStateRepository repository;

    private final EntityManager entityManager;

    String newLine = System.getProperty("line.separator");

    @Autowired
    public S3ObjectSummaryStateService(S3ObjectSummaryStateRepository repository, EntityManager entityManager) {
        this.repository = repository;
        this.entityManager = entityManager;
    }


    @Transactional(readOnly = true)
    public List<S3ObjectSummaryState> getAllS3ObjectSummaryState(String sourceReportType) {
        List<S3ObjectSummaryState> s3ObjectSummaryStateList = Lists.newArrayList();
        try (Stream<S3ObjectSummaryState> stream = repository.findAllS3ObjectSummaryStateByReportType(sourceReportType)) {
            stream.forEach(element -> {
                s3ObjectSummaryStateList.add(element);
                entityManager.detach(element);
            });
        }
        return s3ObjectSummaryStateList;
    }

    @Transactional(readOnly = true)
    public List<S3ObjectSummaryState> getAllS3ObjectSummaryStateByDate(String sourceReportType) {
        List<S3ObjectSummaryState> s3ObjectSummaryStateList = Lists.newArrayList();
        try (Stream<S3ObjectSummaryState> stream = repository.findAllS3ObjectSummaryStateByReportTypeByDate(sourceReportType)) {
            stream.forEach(element -> {
                s3ObjectSummaryStateList.add(element);
                entityManager.detach(element);
            });
        }
        return s3ObjectSummaryStateList;
    }

    @Transactional
    public void insertS3ObjectSummaryState(List<S3ObjectSummary> s3ObjectSummary,String pipeLineRunId,String sourceReportType) {

        List<S3ObjectSummaryState> s3ObjectSummaryStateList = Lists.newArrayList();

        for (S3ObjectSummary s3ObjectSummaries : s3ObjectSummary) {
            S3ObjectSummaryState objectSummaryState;
            objectSummaryState = new S3ObjectSummaryState(new S3ObjectSummaryStateId(pipeLineRunId,s3ObjectSummaries.getBucketName(),s3ObjectSummaries.getKey(),s3ObjectSummaries.getETag(),s3ObjectSummaries.getSize(),s3ObjectSummaries.getLastModified()));
            //objectSummaryState.setPipelineRunId(pipeLineRunId);
            objectSummaryState.setSourceReportType(sourceReportType);
            s3ObjectSummaryStateList.add(objectSummaryState);

        }

        log.info(ANSI_GREEN + newLine + "Below are the list of fileobjects moved to hdfs " + newLine +
                        s3ObjectSummaryStateList.toString() +ANSI_RESET);

        repository.saveAllAndFlush(s3ObjectSummaryStateList);
    }

}