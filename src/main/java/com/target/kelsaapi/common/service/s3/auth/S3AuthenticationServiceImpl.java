package com.target.kelsaapi.common.service.s3.auth;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.util.textFormatterInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service("s3AuthenticationService")
@Slf4j
public class S3AuthenticationServiceImpl implements S3AuthenticationService, textFormatterInterface {

    public AmazonS3 s3Client(String profileName,String regionName,String bucketName,String credentialsFileLocation) throws AuthenticationException {

        try {
            log.info("Authenticate with profile name=> " + profileName );
            log.info("Authenticate with RegionName=> " + regionName );
            log.info("Authenticate with bucket name=> " + bucketName );
            log.info("credentialsFileLocation=>" + credentialsFileLocation);
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSCredentialsProviderChain(new ProfileCredentialsProvider(profileName),
                            new PropertiesFileCredentialsProvider(credentialsFileLocation)))
                    .withRegion(regionName)
                    .build();

            return s3;

        }catch (AmazonS3Exception e) {
            throw new AuthenticationException(e.getMessage(), e.getCause());
        }
        catch (AmazonServiceException e) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + e.getMessage());
            log.error("HTTP Status Code: " + e.getStatusCode());
            log.error("AWS Error Code:   " + e.getErrorCode());
            log.error("Error Type:       " + e.getErrorType());
            log.error("Request ID:       " + e.getRequestId());
            throw new AuthenticationException(e.getMessage(), e.getCause());
        }
    }

    public AmazonS3 s3Client(String accessKey,String secretKey,String profileName,String regionName,String bucketName) throws AuthenticationException {

        try {
            log.info("Authenticate with profile name=> " + profileName);
            log.info("Authenticate with RegionName=> " + regionName);
            log.info("Authenticate with bucket name=> " + bucketName);
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonS3 s3 = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(regionName)
                    .build();

            return s3;

        } catch (AmazonS3Exception e) {
            throw new AuthenticationException(e.getMessage(), e.getCause());
        }
        catch (AmazonServiceException e) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + e.getMessage());
            log.error("HTTP Status Code: " + e.getStatusCode());
            log.error("AWS Error Code:   " + e.getErrorCode());
            log.error("Error Type:       " + e.getErrorType());
            log.error("Request ID:       " + e.getRequestId());
            throw new AuthenticationException(e.getMessage(), e.getCause());
        }
    }

    public AmazonS3 s3Client(String accessKey,String secretKey,String endPoint,String profileName,String regionName,String bucketName) throws AuthenticationException {

        try {
            log.info("Authenticate with profile name=> " + profileName);
            log.info("Authenticate with RegionName=> " + regionName);
            log.info("Authenticate with bucket name=> " + bucketName);
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(
                            new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, regionName))
                    //new EndpointConfiguration("s3-us-west-2.amazonaws.com", ""us-east-2"))
                    //new EndpointConfiguration("http://s3.amazonaws.com", ""us-east-2"))
                    .withPathStyleAccessEnabled(true)
                    .build();

            return s3;

        } catch (AmazonS3Exception e) {
            throw new AuthenticationException(ANSI_RED + e.getMessage() + ANSI_RESET, e.getCause());
        }
        catch (AmazonServiceException e) {
            log.error("AmazonServiceException");
            log.error("Error Message:    " + e.getMessage());
            log.error("HTTP Status Code: " + e.getStatusCode());
            log.error("AWS Error Code:   " + e.getErrorCode());
            log.error("Error Type:       " + e.getErrorType());
            log.error("Request ID:       " + e.getRequestId());
            throw new AuthenticationException(e.getMessage(), e.getCause());
        }
    }

}