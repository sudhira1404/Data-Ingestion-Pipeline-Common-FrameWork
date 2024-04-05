package com.target.kelsaapi.common.vo.s3;

import lombok.Data;
import lombok.Getter;

@Data
public class S3DbParam {

    //static Boolean s3DbParamFlag;
    @Getter
    static String s3PrefixPath;
    @Getter
    static Boolean dirDownload;
    @Getter
    static Boolean xferMgrSingleFileDownload;
    @Getter
    static Boolean s3PrefixSqlFunction;
    @Getter
    static Boolean compareWithPrevLoadedFilesCheckByCurrentDate;
    @Getter
    static Boolean compareWithPrevLoadedFilesCheck;
    @Getter
    static Boolean abortNoFileFound;
    @Getter
    static Integer fileAge;
    @Getter
    static String reportType;
    @Getter
    static Boolean splitFile;
    @Getter
    static Boolean splitFileCompress;



    public S3DbParam(String s3KeyNamePath, Boolean dirDownload, Boolean xferMgrSingleFileDownload,Boolean s3PrefixSqlFunction,
                     Boolean compareWithPrevLoadedFilesCheckByCurrentDate,Boolean compareWithPrevLoadedFilesCheck,Boolean abortNoFileFound,
                     Integer fileAge,String reportType,Boolean splitFile,Boolean splitFileCompress ) {

        this.s3PrefixPath = s3KeyNamePath;
        this.dirDownload = dirDownload;
        this.xferMgrSingleFileDownload = xferMgrSingleFileDownload;
        this.s3PrefixSqlFunction = s3PrefixSqlFunction;
        this.compareWithPrevLoadedFilesCheckByCurrentDate = compareWithPrevLoadedFilesCheckByCurrentDate;
        this.compareWithPrevLoadedFilesCheck = compareWithPrevLoadedFilesCheck;
        this.abortNoFileFound = abortNoFileFound;
        this.fileAge = fileAge;
        this.reportType = reportType;
        this.splitFile = splitFile;
        this.splitFileCompress = splitFileCompress;

    }

}
