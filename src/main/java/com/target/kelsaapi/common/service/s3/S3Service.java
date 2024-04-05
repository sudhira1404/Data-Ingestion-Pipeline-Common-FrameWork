package com.target.kelsaapi.common.service.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.target.kelsaapi.common.vo.s3.S3BucketParam;

import java.io.File;
import java.util.List;

public interface S3Service {
    List<S3ObjectSummary> downloadObjects(String prefix, Boolean dirDownload,
                                          Boolean xferMgrSingleFileDownload, Boolean compareWithPrevLoadedFilesCheckByCurrentDate, Boolean compareWithPrevLoadedFilesCheck, Boolean abortNoFileFound, Integer fileAge,
                                          String reportType, File tempFileDirectory, S3BucketParam s3BucketParam, AmazonS3 s3Client,
                                          String  targetFilePath, Boolean splitFile, Boolean splitFileCompress, Integer maxLinesBeforeFlush,
                                          Long maxSizeBeforeNewFile
                                          ) throws Exception;
}
