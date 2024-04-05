package com.target.kelsaapi.common.vo.s3;

import lombok.Data;

@Data
public class S3BucketParam {


    private String profileName;
    private String bucketName;
    private String regionName;


    public S3BucketParam(String profileName, String bucketName, String regionName) {

        this.profileName = profileName;
        this.bucketName = bucketName;
        this.regionName = regionName;


    }

    public String toString() {
        return "S3BucketParam{bucketName='" + this.bucketName + '\'' + ", profileName='" + this.profileName + '\'' + ", regionName='" + this.regionName + '}';
    }
}
