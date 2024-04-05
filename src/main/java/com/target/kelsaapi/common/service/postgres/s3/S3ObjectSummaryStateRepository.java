package com.target.kelsaapi.common.service.postgres.s3;


import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryState;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryStateId;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;

import java.util.stream.Stream;

import static org.hibernate.jpa.AvailableHints.*;

@Repository("s3ObjectSummaryStateRepository")
public interface S3ObjectSummaryStateRepository extends JpaRepository<S3ObjectSummaryState, S3ObjectSummaryStateId> {


    @Query("SELECT  e FROM S3ObjectSummaryState e " +
            "WHERE e.sourceReportType = ?1 ")
    @QueryHints(value = {
            @QueryHint(name = HINT_FETCH_SIZE, value = "500"),
            @QueryHint(name = HINT_CACHEABLE, value = "false"),
            @QueryHint(name = HINT_READ_ONLY, value = "true")
    })
    Stream<S3ObjectSummaryState> findAllS3ObjectSummaryStateByReportType(String sourceReportType);


    @Query("SELECT  e FROM S3ObjectSummaryState e " +
            "WHERE e.sourceReportType = ?1 " +
            "AND  e.createdDate = current_date")
    @QueryHints(value = {
            @QueryHint(name = HINT_FETCH_SIZE, value = "500"),
            @QueryHint(name = HINT_CACHEABLE, value = "false"),
            @QueryHint(name = HINT_READ_ONLY, value = "true")
    })
    Stream<S3ObjectSummaryState> findAllS3ObjectSummaryStateByReportTypeByDate(String sourceReportType);
}