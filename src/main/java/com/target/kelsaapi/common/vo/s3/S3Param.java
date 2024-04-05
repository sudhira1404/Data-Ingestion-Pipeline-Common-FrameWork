package com.target.kelsaapi.common.vo.s3;

import lombok.Data;

@Data
public class S3Param {

    private String s3KeyNamePath;
    private Boolean dirDownload;
    private Boolean xferMgrSingleFileDownload;
    private Boolean s3PrefixSqlFunction;
    private Boolean compareWithPrevLoadedFilesCheckByCurrentDate;
    private Boolean compareWithPrevLoadedFilesCheck;
    private Boolean abortNoFileFound;
    private Integer fileAge;
    private String reportType;
    private Boolean splitFile;
    private Boolean splitFileCompress;

    public S3Param(String s3KeyNamePath, Boolean dirDownload, Boolean xferMgrSingleFileDownload, Boolean s3PrefixSqlFunction,
                   Boolean compareWithPrevLoadedFilesCheckByCurrentDate, Boolean compareWithPrevLoadedFilesCheck, Boolean abortNoFileFound,
                   Integer fileAge, String reportType, Boolean splitFile, Boolean splitFileCompress ) {

        this.s3KeyNamePath = s3KeyNamePath;
        this.dirDownload = dirDownload;
        this.xferMgrSingleFileDownload = xferMgrSingleFileDownload;
        this.s3PrefixSqlFunction = s3PrefixSqlFunction;
        this.compareWithPrevLoadedFilesCheckByCurrentDate = compareWithPrevLoadedFilesCheckByCurrentDate;
        this.compareWithPrevLoadedFilesCheck = compareWithPrevLoadedFilesCheck;
        this.abortNoFileFound=abortNoFileFound;
        this.fileAge = fileAge;
        this.reportType = reportType;
        this.splitFile = splitFile;
        this.splitFileCompress = splitFileCompress;

    }

}
