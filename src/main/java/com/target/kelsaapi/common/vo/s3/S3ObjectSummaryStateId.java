package com.target.kelsaapi.common.vo.s3;

import jakarta.persistence.Column;
import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

import static com.target.kelsaapi.common.util.S3Utils.dateToString;

@Data
public class S3ObjectSummaryStateId implements Serializable {

    @Id
    @Column(name = "pipeline_run_id")
    private String pipelineRunId;
    @Column(name = "bucketname")
    private String bucketName;
    @Id
    private String key;
    @Id
    private String etag;
    private String size;
    @Column(name="lastmodified")
    private String lastModified;


    public S3ObjectSummaryStateId(String pipelineRunId,String bucketName,String key,String etag,Long size,Date lastModified ) {

        String lastModifiedWithString = dateToString(lastModified);

        this.pipelineRunId = pipelineRunId;
        this.bucketName =bucketName;
        this.key =key;
        this.etag =etag;
        this.size =size.toString();
        this.lastModified =lastModifiedWithString;

    }

    public S3ObjectSummaryStateId() {

    }

}
