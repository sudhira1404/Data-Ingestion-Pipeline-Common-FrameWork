package com.target.kelsaapi.pipelines;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.target.kelsaapi.common.config.JdbcConfig;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.NotSupportedException;
import com.target.kelsaapi.common.service.postgres.s3.S3ObjectSummaryStateService;
import com.target.kelsaapi.common.service.s3.S3Service;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.textFormatterInterface;
import com.target.kelsaapi.common.vo.s3.S3BucketParam;
import com.target.kelsaapi.common.vo.s3.S3DbParam;
import com.target.kelsaapi.common.vo.s3.S3Param;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public abstract class S3Consumer extends EndPointConsumer implements textFormatterInterface,S3ConsumerInterface {

    final private S3Service s3Service;

    final private S3ObjectSummaryStateService s3ObjectSummaryStateService;

    private final JdbcConfig config;

    public S3Consumer(ApplicationContext context, String pipelineRunId) {
        super(context, pipelineRunId);
        this.s3Service = context.getBean(S3Service.class);
        this.s3ObjectSummaryStateService = context.getBean(S3ObjectSummaryStateService.class);
        this.config = context.getBean(JdbcConfig.class);
    }

    public void runPipeline(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch, S3BucketParam s3BucketParam, AmazonS3 s3Client)
            throws Exception {

        //Begin the timer
        CommonUtils.timerSplit(stopWatch, "Initialization");

        String profileName = s3BucketParam.getProfileName();
        String regionName = s3BucketParam.getRegionName();
        String bucketName = s3BucketParam.getBucketName();

        File tempFileDirectory = new File(CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.S3,null));
        String targetFileNew;
        String msg;
        List<S3ObjectSummary> listFileObjects;
        List<S3ObjectSummary> listFilteredFileObjects = new ArrayList<>();

        if (targetFile.lastIndexOf('/') == targetFile.length() - 1) {
            targetFileNew = targetFile;
        } else {
            targetFileNew = targetFile + File.separator;
        }

        log.info("localTempFileDirectory where the files will be downloaded " + tempFileDirectory);

        try {
            S3Param s3Param = deriveS3KeyNamePath(startDate, reportType, profileName, regionName, bucketName);

            String s3KeyNamePath = s3Param.getS3KeyNamePath();
            boolean dirDownload = s3Param.getDirDownload();
            boolean xferMgrSingleFileDownload = s3Param.getXferMgrSingleFileDownload();
            boolean compareWithPrevLoadedFilesCheckByCurrentDate = s3Param.getCompareWithPrevLoadedFilesCheckByCurrentDate();
            boolean compareWithPrevLoadedFilesCheck = s3Param.getCompareWithPrevLoadedFilesCheck();
            boolean abortNoFileFound = s3Param.getAbortNoFileFound();
            boolean splitFile = s3Param.getSplitFile();
            boolean splitFileCompress = s3Param.getSplitFileCompress();
            Integer fileAge = s3Param.getFileAge();
            Integer maxLinesBeforeFlush = pipelineConfig.apiconfig.source.s3SwitchBoard.maxLinesBeforeFlush;
            Long maxSizeBeforeNewFile = pipelineConfig.apiconfig.source.s3SwitchBoard.maxSizeBeforeNewFile;
            if (!splitFile) {
                msg = "Ingest from S3";
            } else {
                msg = "Ingest from S3 and move to hdfs";
            }

            CommonUtils.timerSplit(stopWatch, msg);

            log.info("Call s3 api to download contents of the file from => " + s3KeyNamePath);
            listFileObjects = s3Service.downloadObjects(s3KeyNamePath, dirDownload, xferMgrSingleFileDownload,
                    compareWithPrevLoadedFilesCheckByCurrentDate, compareWithPrevLoadedFilesCheck, abortNoFileFound, fileAge, reportType, tempFileDirectory, s3BucketParam, s3Client,
                    targetFile, splitFile, splitFileCompress, maxLinesBeforeFlush, maxSizeBeforeNewFile);

            String fileDownloadStatus = pipelineConfig.apiconfig.getSource().getS3SwitchBoard().getFileDownloadStatus();

            if (!splitFile) {
                msg = "Write to HDFS(" + fileDownloadStatus + ")";
            } else {
                msg = "HDFS move status(" + fileDownloadStatus + ")";
            }

            CommonUtils.timerSplit(stopWatch, msg);
            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;

            if (!splitFile) {
                for (S3ObjectSummary file : listFileObjects) {
                    File key = new File(file.getKey());
                    String filename = key.getName();
                    String targetFilePath = targetFileNew + filename;
                    String localFileNamePath = tempFileDirectory + "/" + filename;
                    if (dirDownload) {
                        localFileNamePath = tempFileDirectory + "/" + file.getKey();
                    }
                    Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFilePath, 3, localFileNamePath, cleanupTempFile);
                    if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                        try {
                            throw new IOException("All write attempts to HDFS failed for " + targetFilePath);
                        } catch (IOException e) {
                            log.error(e.getMessage(), e.getCause());
                        }
                    } else {
                        listFilteredFileObjects.add(file);
                    }
                }
            } else {
                listFilteredFileObjects = listFileObjects;
            }

            log.info("Insert the S3ObjectSummaryState to PGDB mdf_s3_objectsummary");
            s3ObjectSummaryStateService.insertS3ObjectSummaryState(listFilteredFileObjects, pipelineRunId, reportType);

        } catch(FileNotFoundException fna) {
            CommonUtils.timerSplit(stopWatch, pipelineConfig.apiconfig.getSource().getS3SwitchBoard().getFileDownloadStatus());
            throw new FileNotFoundException(fna.getMessage());
        } catch(Exception e) {
            log.error(e.getMessage(),e.getCause());
            log.error(Arrays.toString(e.getStackTrace()));
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

    }


    private ApplicationConstants.S3ReportTypes toReportType(String reportType) throws IllegalArgumentException {
        ApplicationConstants.S3ReportTypes s3ReportTypes = ApplicationConstants.S3ReportTypes
                .valueOf(reportType.toUpperCase());
        log.info("Report type {} requested =>", s3ReportTypes);
        return s3ReportTypes;
    }


    private String addTrailingSlash(String s3PrefixPath) {
        if (s3PrefixPath.charAt(s3PrefixPath.length() - 1) != File.separatorChar) {
            s3PrefixPath += File.separator;
        }

        return s3PrefixPath;
    }

    private String formatS3KeyNamePath(String s3PrefixPath, String startDate, Boolean dirDownload,Boolean s3PrefixSqlFunction) {

        String s3PrefixPathFileExt = FilenameUtils.getExtension(s3PrefixPath);
        if (dirDownload){
            s3PrefixPath = addTrailingSlash(s3PrefixPath);
        }

        String s3KeyNamePath;

        if (s3PrefixPathFileExt.isEmpty()) {
            if (!s3PrefixSqlFunction) {
                s3KeyNamePath = s3PrefixPath + startDate + "/";
            } else {
                s3KeyNamePath = addTrailingSlash(s3PrefixPath);
            }
        }
        else { s3KeyNamePath = s3PrefixPath;}

        return s3KeyNamePath;

    }

    private S3Param deriveS3KeyNamePath(String startDate, String reportType, String profileName, String regionName, String bucketName)
            throws RuntimeException, SQLException, NotSupportedException, ParseException {

        //Adding validation of date format to prevent sql injection
        CommonUtils.validateDateFormat(startDate);

        String level;
        Boolean dirDownload;
        Boolean xferMgrSingleFileDownload;
        Boolean s3PrefixSqlFunction;
        Boolean compareWithPrevLoadedFilesCheckByCurrentDate;
        Boolean compareWithPrevLoadedFilesCheck;
        Boolean abortNoFileFound;
        Integer fileAge;
        String s3KeyNamePath;
        String s3PrefixPath;
        Boolean splitFile;
        Boolean splitFileCompress;
        String newLine = System.getProperty("line.separator");

        if (S3DbParam.getReportType().isEmpty()){
            log.info(ANSI_GREEN + "Parameters(prefix and others) will be read from the application constants" + ANSI_RESET);
            log.debug(ANSI_GREEN + "Had you chosen to get from PG, the URL would have been {}, the Username would have been {}, and the Password would have been {}", config.getUrl(), config.getUsername(), config.getPassword());
            level = toReportType(reportType).toString();
            String name = level.toUpperCase();
            s3PrefixPath = ApplicationConstants.S3ReportTypes.valueOf(name).getS3Prefix();
            dirDownload = ApplicationConstants.S3ReportTypes.valueOf(level).getDirDownload();
            xferMgrSingleFileDownload = ApplicationConstants.S3ReportTypes.valueOf(level).getXferMgrSingleFileDownload();
            s3PrefixSqlFunction = ApplicationConstants.S3ReportTypes.valueOf(level).getS3PrefixSqlFunction();
            compareWithPrevLoadedFilesCheckByCurrentDate = ApplicationConstants.S3ReportTypes.valueOf(level).getCompareWithPrevLoadedFilesCheckByCurrentDate();
            compareWithPrevLoadedFilesCheck = ApplicationConstants.S3ReportTypes.valueOf(level).getCompareWithPrevLoadedFilesCheck();
            abortNoFileFound  = ApplicationConstants.S3ReportTypes.valueOf(level).getAbortNoFileFound();
            fileAge = ApplicationConstants.S3ReportTypes.valueOf(level).getFileAge();
            splitFile = ApplicationConstants.S3ReportTypes.valueOf(level).getSplitfile();
            splitFileCompress = ApplicationConstants.S3ReportTypes.valueOf(level).getSplitfilecompress();
        }
        else {
            level = S3DbParam.getReportType();
            log.info(ANSI_GREEN + "Parameters(prefix and others) will be read from the PGDB not from the application constants." + ANSI_RESET);
            log.info(String.format(ANSI_GREEN + "If parameters are to be read from the application constants,then make the actv_f=N in database for the reporttype parameterentry %s " + newLine +
                    "and have reporttype parameterentry %s in the application constants" ,level,level) + ANSI_RESET);

            s3PrefixPath = S3DbParam.getS3PrefixPath();
            dirDownload = S3DbParam.getDirDownload();
            xferMgrSingleFileDownload = S3DbParam.getXferMgrSingleFileDownload();
            s3PrefixSqlFunction = S3DbParam.getS3PrefixSqlFunction();
            compareWithPrevLoadedFilesCheckByCurrentDate = S3DbParam.getCompareWithPrevLoadedFilesCheckByCurrentDate();
            compareWithPrevLoadedFilesCheck = S3DbParam.getCompareWithPrevLoadedFilesCheck();
            abortNoFileFound = S3DbParam.getAbortNoFileFound();
            fileAge = S3DbParam.getFileAge();
            splitFile =  S3DbParam.getSplitFile();
            splitFileCompress = S3DbParam.getSplitFileCompress();
        }

        if (s3PrefixSqlFunction) {
            Connection connect = DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
            String firstStringInPrefix = s3PrefixPath.trim().substring(0, s3PrefixPath.indexOf(' '));
            if (firstStringInPrefix.toUpperCase().contains("SELECT") &&  !firstStringInPrefix.toUpperCase().contains("WHERE")) {
                String sqlPrefix = s3PrefixPath.replaceAll("\\$startDate", startDate);
                log.info(ANSI_GREEN + "Executing the sql to evaluate the prefix from table mdf_s3_parameter=>" + sqlPrefix + ANSI_RESET);
                try (Connection conn = connect;
                     PreparedStatement p = conn.prepareStatement(sqlPrefix))
                {
                    try (ResultSet rs = p.executeQuery()) {
                        rs.next();
                        s3PrefixPath = rs.getString(1);

                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    log.error(ANSI_RED + "SQL Error Message while evaluating the prefix from table mdf_s3_parameter: " + e.getMessage() + newLine +
                            "Correct the sql and rerun the process=>" + sqlPrefix + ANSI_RESET );
                    throw new SQLException("SQL Error Message to get prefix path from db");

                }

            }
            else {log.info("Prefix parameter provided is not a select query or select contains where clause ,so will not be executed:" + s3PrefixPath);
                throw new NotSupportedException(ANSI_RED + "Prefix parameter provided is not a select query or select contains where clause,so will not be executed.Provide a select query" +
                        " which evaluates to a prefix path when query is executed to continue the flow" + ANSI_RESET);

            }
        }

        s3KeyNamePath = formatS3KeyNamePath(s3PrefixPath,startDate,dirDownload,s3PrefixSqlFunction);

        String text = String.format("List of parameters read for reporttype %s:" + newLine + "prefix=%s" + newLine + "dirDownload=%s" + newLine + "xferMgrSingleFileDownload=%s" + newLine + "s3PrefixSqlFunction=%s" + newLine + "compareWithPrevLoadedFilesCheckByCurrentDate=%s" + newLine + "compareWithPrevLoadedFilesCheck=%s" + newLine + "abortNoFileFound=%s" + newLine + "fileAge=%s" + newLine + "profileName=%s" + newLine + "bucketName=%s" + newLine + "regionName=%s" + newLine + "splitFile=%s" + newLine + "splitFileCompress=%s" ,
                level,s3KeyNamePath,dirDownload,xferMgrSingleFileDownload,s3PrefixSqlFunction,compareWithPrevLoadedFilesCheckByCurrentDate,compareWithPrevLoadedFilesCheck,abortNoFileFound,fileAge,profileName,bucketName,regionName,splitFile,splitFileCompress);
        log.info(ANSI_GREEN + text + ANSI_RESET);

        return new S3Param(s3KeyNamePath, dirDownload, xferMgrSingleFileDownload,s3PrefixSqlFunction,compareWithPrevLoadedFilesCheckByCurrentDate,compareWithPrevLoadedFilesCheck,abortNoFileFound,fileAge,level,splitFile,splitFileCompress);

    }

}