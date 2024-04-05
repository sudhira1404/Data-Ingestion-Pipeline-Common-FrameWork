package com.target.kelsaapi.common.vo.s3;

public interface S3DbParamState {
    String getSourceReportType();
    String getActvF();
    String getPrefix();
    Boolean getDirDownload();
    Boolean getXferMgrSingleFileDownload();
    Boolean getS3PrefixSqlFunction();
    Boolean getCompareWithPrevLoadedFilesCheckByCurrentDate();
    Boolean getCompareWithPrevLoadedFilesCheck();
    Boolean getAbortNoFileFound();
    Integer getFileAge();
    Boolean getSplitFile();
    Boolean getSplitFileCompress();

}
