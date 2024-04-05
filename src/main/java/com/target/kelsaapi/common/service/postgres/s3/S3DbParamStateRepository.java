package com.target.kelsaapi.common.service.postgres.s3;

import com.target.kelsaapi.common.vo.s3.S3DbParamState;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryState;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryStateId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository("getS3DbParamStateRepository")
public interface S3DbParamStateRepository extends JpaRepository<S3ObjectSummaryState, S3ObjectSummaryStateId> {


    @Query(value = "select trim (source_report_type) as sourceReportType," +
            "              trim (actv_f) as actvF ," +
            "              trim (prefix) as prefix ," +
            "             dirdownload as dirDownload ," +
            "             xfermgrsinglefiledownload as xferMgrSingleFileDownload ," +
            "             s3prefixsqlfunction as s3prefixsqlfunction," +
            "             comparewithprevloadedfilescheckbycurrentDate as compareWithPrevLoadedFilesCheckByCurrentDate," +
            "             comparewithprevloadedfilescheck as compareWithPrevLoadedFilesCheck," +
            "             abortnofilefound as abortnofilefound," +
            "             fileage as fileAge," +
            "             splitfile as splitFile," +
            "             splitfilecompress as splitFileCompress" +
            "    from mdf_s3_parameter a where lower(a.source_report_type) = lower(:sourceReportType)" +
            "                              and lower(a.actv_f) = lower('Y')",
            nativeQuery = true)
    S3DbParamState getS3DbParamState(@Param("sourceReportType") String sourceReportType);



}