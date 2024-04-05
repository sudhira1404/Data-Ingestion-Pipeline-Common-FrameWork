package com.target.kelsaapi.common.vo.s3;

import com.amazonaws.services.s3.model.Owner;
import lombok.Data;

import java.util.Date;

@Data
public class S3FileObjectSummary {

    protected String bucketName;
    protected String key;
    protected String eTag;
    protected long size;
    protected Date lastModified;
    protected String storageClass;
    protected Owner owner;


    public String toString() {
        return "S3FileObjectSummary{bucketName='" + this.bucketName + '\'' + ", key='" + this.key + '\'' + ", eTag='" + this.eTag + '\'' + ", size=" + this.size + ", lastModified=" + this.lastModified + ", storageClass='" + this.storageClass + '\'' + ", owner=" + this.owner + '}';
    }


}
