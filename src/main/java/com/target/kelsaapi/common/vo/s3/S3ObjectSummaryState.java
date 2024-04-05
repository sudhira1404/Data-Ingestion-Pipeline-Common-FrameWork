package com.target.kelsaapi.common.vo.s3;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.springframework.lang.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;


@Entity
@Table(name="mdf_s3_objectsummary")
@Getter
@Setter
@IdClass(S3ObjectSummaryStateId.class)
public class S3ObjectSummaryState implements Serializable {

    @Id
    @Column(name = "pipeline_run_id")
    protected String pipelineRunId;

    @Basic
    @Column(name = "source_report_type")
    protected String sourceReportType;

    @Id
    @Column(name = "key")
    protected String key;

    @Id
    @Column(name = "etag")
    protected String etag;

    @Column(name = "bucketname")
    protected String bucketName;

    @Column(name = "size")
    protected String size;

    @Column(name = "lastmodified")
    protected String lastModified;

    @Column(name = "created_timestamp")
    @Nullable
    @CreationTimestamp
    protected Instant createdTimestamp;

    @Column(name = "created_date")
    @Nullable
    protected LocalDate createdDate=java.time.LocalDate.now();


    public S3ObjectSummaryState(S3ObjectSummaryStateId s3ObjectSummaryStateId) {


        this.pipelineRunId = s3ObjectSummaryStateId.getPipelineRunId();
        this.bucketName = s3ObjectSummaryStateId.getBucketName();
        this.key = s3ObjectSummaryStateId.getKey();
        this.etag = s3ObjectSummaryStateId.getEtag();
        this.size = s3ObjectSummaryStateId.getSize();
        this.lastModified = s3ObjectSummaryStateId.getLastModified();

    }


    public S3ObjectSummaryState() {

    }

    public String toString() {
        return "S3FileObjectSummaryState{bucketName='" + this.bucketName + '\'' + ", key='" + this.key + '\'' + ", eTag='" + this.etag + '\'' + ", size=" + this.size + ", lastModified=" + this.lastModified + ", pipelineRunId='" + this.pipelineRunId + '}';
    }

}
