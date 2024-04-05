package com.target.kelsaapi.common.service.s3.auth;

import com.amazonaws.services.s3.AmazonS3;
import com.target.kelsaapi.common.exceptions.AuthenticationException;

public interface S3AuthenticationService {
    AmazonS3 s3Client(String profileName,String regionName,String bucketName,String credentialsFileLocation) throws AuthenticationException;
    AmazonS3 s3Client(String accessKey,String secretKey,String profileName,String regionName,String bucketName) throws AuthenticationException;
    AmazonS3 s3Client(String accessKey,String secretKey,String endPoint,String profileName,String regionName,String bucketName) throws AuthenticationException;
}
