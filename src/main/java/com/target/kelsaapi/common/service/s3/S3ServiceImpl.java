package com.target.kelsaapi.common.service.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.target.kelsaapi.common.exceptions.AuthenticationException;
import com.target.kelsaapi.common.service.postgres.s3.S3ObjectSummaryStateService;
import com.target.kelsaapi.common.service.s3.aws.S3DownloadService;
import com.target.kelsaapi.common.util.S3Utils;
import com.target.kelsaapi.common.util.textFormatterInterface;
import com.target.kelsaapi.common.vo.s3.S3BucketParam;
import com.target.kelsaapi.common.vo.s3.S3ObjectSummaryState;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service("s3Service")
class S3ServiceImpl implements S3Service, textFormatterInterface {

    final private PipelineConfig config;

    private final S3ObjectSummaryStateService s3ObjectSummaryStateService;

    private final S3DownloadService s3DownloadService;

    @Autowired
    public S3ServiceImpl(PipelineConfig config, S3ObjectSummaryStateService s3ObjectSummaryStateService,S3DownloadService s3DownloadService) {
        this.config = config;
        this.s3ObjectSummaryStateService = s3ObjectSummaryStateService;
        this.s3DownloadService = s3DownloadService;
    }

    public  List<S3ObjectSummary> downloadObjects (String keyName, Boolean dirDownload,
                                                   Boolean xferMgrSingleFileDownload, Boolean compareWithPrevLoadedFilesCheckByCurrentDate, Boolean compareWithPrevLoadedFilesCheck, Boolean abortNoFileFound,
                                                   Integer fileAge, String reportType, File tempFileDirectory, S3BucketParam s3BucketParam, AmazonS3 s3Client,
                                                   String  targetFilePath, Boolean splitFile,Boolean splitFileCompress,Integer maxLinesBeforeFlush,
                                                   Long maxSizeBeforeNewFile
    ) throws IOException, ParseException, AuthenticationException {

        List<S3ObjectSummary> listFileObjects;
        String fileDownloadStatus;
        List<S3ObjectSummary> listFirstFilteredFileObjects;
        List<S3ObjectSummary> listSecondFilteredFileObjects;
        List<S3ObjectSummary> listThirdFilteredFileObjects = new ArrayList<>();
        String newLine = System.getProperty("line.separator");
        String bucketName =      s3BucketParam.getBucketName();

        log.info("Getting list of file objects from S3 bucket=>" + bucketName + " the path => "  + keyName );

        listFileObjects = S3Utils.getListOfFilesObjectsFromFolder(s3Client,bucketName, keyName);

        fileNotAvailableException(listFileObjects,"s3Bucket",keyName,true);

//        if (listFileObjects.isEmpty() && abortNoFileFound)
//         {
//            String msg = String.format(ANSI_RED+"There are zero files in the s3 path %s" + ANSI_RESET, keyName);
//            throw new FileNotFoundException(msg);
//        }

        if (dirDownload) {
            s3DownloadService.downloadDir(s3Client,bucketName,
                    keyName, false, tempFileDirectory);
            listThirdFilteredFileObjects = listFileObjects;
            fileNotAvailableException(listThirdFilteredFileObjects,"dirDownload",keyName,true);
        } else {
            if (compareWithPrevLoadedFilesCheckByCurrentDate) {
                log.info("Will compare current list of filenames and etag(s3 checksum value of the file) with the previously loaded files stored in table mdf_s3_objectsummary" +
                        " for the current date and download only the difference");
                List<S3ObjectSummaryState> prevS3ObjectSummaryState =s3ObjectSummaryStateService.getAllS3ObjectSummaryStateByDate(reportType);
                listFirstFilteredFileObjects = S3Utils.curPrevLoadedFileObjectsEtagCheck(prevS3ObjectSummaryState,listFileObjects,reportType);
                fileNotAvailableException(listFirstFilteredFileObjects,"compareWithPrevLoadedFilesCheckByCurrentDate",keyName,true);
            } else {
                listFirstFilteredFileObjects =listFileObjects ;
            }

            if (compareWithPrevLoadedFilesCheck) {
                log.info("Will compare current list of filenames and etag(s3 checksum value of the file) with the previously loaded all files stored in table mdf_s3_objectsummary" +
                        " and download only the difference");
                List<S3ObjectSummaryState> prevS3ObjectSummaryState =s3ObjectSummaryStateService.getAllS3ObjectSummaryState(reportType);
                listSecondFilteredFileObjects = S3Utils.curPrevLoadedFileObjectsEtagCheck(prevS3ObjectSummaryState,listFirstFilteredFileObjects,reportType);
                fileNotAvailableException(listSecondFilteredFileObjects,"compareWithPrevLoadedFilesCheck",keyName,true);

            } else {
                listSecondFilteredFileObjects =listFirstFilteredFileObjects ;
            }
            for (S3ObjectSummary file : listSecondFilteredFileObjects) {
                String lastModifiedWithString = S3Utils.dateToString(file.getLastModified());
                try {
                    Boolean fileAgeCheck = S3Utils.filterFiles(lastModifiedWithString, fileAge);
                    if (!fileAgeCheck) {
                        log.info("Start downloading the file => " + file.getKey());
                        if (xferMgrSingleFileDownload) {
                            s3DownloadService.downloadFile(s3Client, bucketName, file.getKey(), false, tempFileDirectory);
                        }
                        else {
                            if (!splitFile) {
                                s3DownloadService.downloadFileOrDir(s3Client, bucketName, file.getKey(), tempFileDirectory);
                            } else {
                                s3DownloadService.downloadFileFromUrlMoveToHDFS(s3Client, bucketName, file.getKey(), tempFileDirectory, targetFilePath,
                                        splitFileCompress, maxLinesBeforeFlush, maxSizeBeforeNewFile);
                            }
                        }
                        listThirdFilteredFileObjects.add(file);
                    }
                } catch (ParseException e) {
                    log.error(e.getMessage(),e.getCause());
                    log.error(Arrays.toString(e.getStackTrace()));
                    throw new ParseException(e.getMessage(),e.getErrorOffset());
                }
                catch (IOException e) {
                    log.error(e.getMessage(),e.getCause());
                    log.error(Arrays.toString(e.getStackTrace()));
                    throw new IOException(e.getMessage(),e.getCause());
                }
            }
            fileNotAvailableException(listThirdFilteredFileObjects,"fileAgeCheck",keyName,true);

        }


//        if (listThirdFilteredFileObjects.isEmpty())
//
//        {
//            String text = String.format("There are zero files to be downloaded from the s3 path %s. Check the s3 bucket for files or any filters (fileAge or compareWithPrevLoadedFilesCheck) applied in the process", keyName);
//            log.info( ANSI_YELLOW + newLine + "======================================================================================================================================================================================" + newLine + text +
//                    newLine + "=======================================================================================================================================================================================" + ANSI_RESET);
//            config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(text);
//            throw new FileNotFoundException(text);
//        }
//
//        else {
//
//            log.info(String.format(newLine + " Number of files available in the s3 path %s after applying any input filters=>%s" + newLine +
//                    "Number of files that are downloaded to local path %s=>%s", keyName, listThirdFilteredFileObjects.size(), tempFileDirectory, listThirdFilteredFileObjects.size()));
//            fileDownloadStatus = String.format("Number of files downloaded from the s3 path %s=>%s", keyName, listThirdFilteredFileObjects.size());
//            config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(fileDownloadStatus);
//
//        }

        log.info(String.format(newLine + " Number of files available in the s3 path %s after applying any input filters=>%s" + newLine +
                "Number of files that are downloaded to local path %s=>%s", keyName, listThirdFilteredFileObjects.size(), tempFileDirectory, listThirdFilteredFileObjects.size()));
        fileDownloadStatus = String.format("Number of files downloaded from the s3 path %s=>%s", keyName, listThirdFilteredFileObjects.size());
        config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(fileDownloadStatus);


        return listThirdFilteredFileObjects;

    }

    public  void fileNotAvailableException(List<S3ObjectSummary>listFileObjects,String parameterType,String keyName,Boolean abortNoFileFound) throws FileNotFoundException {
        switch (parameterType) {
            case "s3Bucket" : {
                if (listFileObjects.isEmpty() && abortNoFileFound) {String msg = String.format(ANSI_RED+"There are zero files in the s3 path %s" + ANSI_RESET, keyName);config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(msg);throw new FileNotFoundException(msg);
                }
            }
            case "compareWithPrevLoadedFilesCheckByCurrentDate" : {
                if (listFileObjects.isEmpty() && abortNoFileFound) {String msg = String.format(ANSI_RED+"There are zero files after applying filter on parameter compareWithPrevLoadedFilesCheckByCurrentDate" + ANSI_RESET);config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(msg);throw new FileNotFoundException(msg);
                }
            }

            case "compareWithPrevLoadedFilesCheck" : {
                if (listFileObjects.isEmpty() && abortNoFileFound) {String msg = String.format(ANSI_RED+"There are zero files after applying filter on parameter compareWithPrevLoadedFilesCheck" + ANSI_RESET);config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(msg);throw new FileNotFoundException(msg);
                }
            }
            case "fileAgeCheck" : {
                if (listFileObjects.isEmpty() && abortNoFileFound) {String msg = String.format(ANSI_RED+"There are zero files after applying filter on parameter fileAgeCheck" + ANSI_RESET);config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(msg);throw new FileNotFoundException(msg);
                }
            }
            case "dirDownload" : {
                if (listFileObjects.isEmpty() && abortNoFileFound) {String msg = String.format(ANSI_RED+"There are zero files after applying filter on parameter dirDownload in the s3 path %s" + ANSI_RESET, keyName);config.apiconfig.source.s3SwitchBoard.setFileDownloadStatus(msg);throw new FileNotFoundException(msg);
                }
            }
        }

    }
}
