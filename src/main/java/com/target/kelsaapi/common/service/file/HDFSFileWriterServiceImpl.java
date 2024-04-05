package com.target.kelsaapi.common.service.file;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.NotFoundException;
import com.target.kelsaapi.common.exceptions.WriterException;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Utilities for writing to HDFS
 */
@Service("hdfsFileWriterService")
@Slf4j
public class HDFSFileWriterServiceImpl implements HDFSFileWriterService {

    private final XenonService xenonService;

    private final PipelineConfig.Hdfsdetails hdfsdetails;

    private final LocalFileWriterService localFileWriter;

    /**
     * Constructor for HDFSFileWriterService. Used by Spring Framework to instantiate and auto-wire dependent beans.
     *
     * @param config The PipelineConfig bean.
     * @param xenonService The XenonService bean.
     * @param localFileWriter The LocalFileWriterService bean.
     */
    @Autowired
    HDFSFileWriterServiceImpl(PipelineConfig config, XenonService xenonService, LocalFileWriterService localFileWriter) {
        this.xenonService = xenonService;
        this.hdfsdetails = config.apiconfig.hdfsdetails;
        this.localFileWriter = localFileWriter;
    }

    /**
     * Writes a local temp file from a single String and transfers the file to HDFS.
     * Wrapper around {@link #writeToHDFS(String, String, Integer, String, Boolean)}
     *
     * <pre>Sets default values for:
     *  writeAttempts - 3
     *  tempFile - {@link ApplicationConstants#TEMPFILE}
     *  cleanupTempFile - true</pre>
     *
     * @param contents The data to write to local file.
     * @param filePath The file path to transfer to in HDFS.
     * @return True if it was successful in writing local temp file and transferring to HDFS. False if not.
     */
    @Override
    public Boolean writeToHDFS(String contents,
                               String filePath) {

        return writeToHDFS(contents, filePath, ApplicationConstants.DEFAULT_MAX_WRITE_ATTEMPTS,
                ApplicationConstants.TEMPFILE, true);
    }

    /**
     * Writes a local temp file from a single String and transfers the file to HDFS.
     *
     * @param contents The data to write to local file.
     * @param filePath The file path to transfer to in HDFS.
     * @param writeAttempts The max number of attempts to try before failing.
     * @param tempFile The local temp file to be transferred to HDFS.
     * @param cleanupTempFile True to delete the tempFile after transferring. False to preserve the tempFile on local disk.
     * @return True if it was successful in writing local temp file and transferring to HDFS. False if not.
     */
    @SneakyThrows
    @Override
    public Boolean writeToHDFS(String contents,
                               String filePath,
                               Integer writeAttempts,
                               String tempFile,
                               Boolean cleanupTempFile) {
        Boolean localFileWriteSuccess = localFileWriter.retryableLocalWrite(contents, tempFile);
        if (Boolean.TRUE.equals(localFileWriteSuccess)) {
            return writeToHDFS(filePath, writeAttempts, tempFile, cleanupTempFile);
        } else {
            return false;
        }
    }

    /**
     * Writes a local temp file from a List of Strings and transfers the file to HDFS.
     *
     * @param contents The data to write to local file.
     * @param filePath The file path to transfer to in HDFS.
     * @param writeAttempts The max number of attempts to try before failing.
     * @param tempFile The local temp file to be transferred to HDFS.
     * @param cleanupTempFile True to delete the tempFile after transferring. False to preserve the tempFile on local disk.
     * @return True if it was successful in writing local temp file and transferring to HDFS. False if not.
     */
    @SneakyThrows
    @Override
    public Boolean writeToHDFS(List<String> contents,
                               String filePath,
                               Integer writeAttempts,
                               String tempFile,
                               Boolean cleanupTempFile) {
        log.debug("Total number of strings to write to disk: " + contents.size());
        Boolean localFileWriteSuccess = localFileWriter.retryableLocalWrite(contents, tempFile);
        if (Boolean.TRUE.equals(localFileWriteSuccess)) {
            return writeToHDFS(filePath, writeAttempts, tempFile, cleanupTempFile);
        } else {
            return false;
        }
    }

    /**
     * Writes a local temp file and transfers the file to HDFS.
     * The local temp file is a tar archive made of one or more data files that is gzip'd.
     *
     * @param filePath The file path to transfer to in HDFS.
     * @param outputGZTarFile The final tar archive that's gzipped and will be sent to HDFS.
     * @param inputFilePaths The path to the directory locally containing one or more data files.
     *                       These files are added to the file specified by outputGZTarFile param.
     * @param writeAttempts The max number of attempts to try before failing.
     * @param cleanupTempFile True to delete the tempFile after transferring. False to preserve the tempFile on local disk.
     * @return True if it was successful in writing local temp file and transferring to HDFS. False if not.
     * @throws WriterException
     * @throws NotFoundException
     */
    @SneakyThrows
    @Override
    public Boolean writeToHDFS(String filePath,
                               String outputGZTarFile,
                               List<Path> inputFilePaths,
                               Integer writeAttempts,
                               Boolean cleanupTempFile) {
        Boolean localWriteSuccess = localFileWriter.retryableLocalWrite(outputGZTarFile, inputFilePaths);
        if (Boolean.TRUE.equals(localWriteSuccess)) {
            return writeToHDFS(filePath, writeAttempts, outputGZTarFile,  cleanupTempFile);
        } else {
            return false;
        }
    }

    /**
     * Writes a local temp file from an InputStream and transfers the file to HDFS.
     *
     * @param contents The data to write to local file.
     * @param filePath The file path to transfer to in HDFS.
     * @param writeAttempts The max number of attempts to try before failing.
     * @param tempFile The local temp file to be transferred to HDFS.
     * @param cleanupTempFile True to delete the tempFile after transferring. False to preserve the tempFile on local disk.
     * @return True if it was successful in writing local temp file and transferring to HDFS. False if not.
     */
    @SneakyThrows
    @Override
    public Boolean writeToHDFS(InputStream contents, String filePath, Integer writeAttempts, String tempFile, Boolean cleanupTempFile) {
        Boolean localWriteSuccess = localFileWriter.retryableLocalWrite(contents, tempFile);
        if (Boolean.TRUE.equals(localWriteSuccess)) {
            return writeToHDFS(filePath, writeAttempts, tempFile, cleanupTempFile);
        } else {
            return false;
        }
    }

    /**
     * Transfers a local file to HDFS.
     *
     * @param filePath The file path to transfer to in HDFS.
     * @param writeAttempts The max number of attempts to try before failing.
     * @param tempFile The local temp file to be transferred to HDFS.
     * @param cleanupTempFile True to delete the tempFile after transferring. False to preserve the tempFile on local disk.
     * @return True if it was successful in transferring the local tempFile to HDFS. False if not.
     */
    @Override
    public Boolean writeToHDFS(String filePath,
                               Integer writeAttempts,
                               String tempFile,
                               Boolean cleanupTempFile) {
        Boolean success = false;
        int attempts = 1;
        log.debug("Number of attempts: " + writeAttempts);
        log.debug("Temp file name received: " + tempFile);
        log.debug("HDFS writer details: " + hdfsdetails);
        log.debug("Target file:" + filePath);
        log.info("Attempting to write to HDFS...");
        while (Boolean.FALSE.equals(success) && attempts <= writeAttempts) {
            log.info("File transfer attempt #{}", attempts);
            if (Boolean.FALSE.equals(xenonService.isFileExists(filePath))){
                log.info("File didn't previously exist, transferring a new file");
                success = xenonService.transferFile(filePath, tempFile, false, true, false);
            } else {
                log.info("File already exists, overwriting prior version to land new data");
                success = xenonService.transferFile(filePath, tempFile, true, true, false);
            }
            attempts+=1;
        }

        if (Boolean.TRUE.equals(cleanupTempFile)) {
            log.info("Cleaning up temp file from local filesystem: {}", tempFile);
            localFileWriter.deleteLocalFile(Paths.get(tempFile));
        } else {
            log.warn("No clean attempted for temp file on local filesystem. Make sure to not preserve this file long-term: {}", tempFile);
        }

        log.debug("Attempts tried: {}", attempts-1);
        log.debug("Max attempts to try: {}", writeAttempts);
        log.debug("Was successful: {}", success);
        if (attempts > writeAttempts && Boolean.FALSE.equals(success)) {
            log.error("Failed to transfer file successfully to remote HDFS filesystem {}",filePath);
            return false;
        } else {
            log.info("Successfully transferred file to remote HDFS filesystem {}",filePath);
            return true;
        }
    }

    /**
     * Transfers a local file to HDFS.
     *
     * @param filePath The file path to transfer to in HDFS.
     * @param tempFile The local temp file to be transferred to HDFS.
     * @param cleanupTempFile True to delete the tempFile after transferring. False to preserve the tempFile on local disk.
     * @return Throws IOException if move failed.
     */
    @Override
    public void writeToHDFS(String filePath, String tempFile, Integer writeAttempts,Boolean cleanupTempFile) throws IOException {

        Boolean xenonLoadedFlag=true;

        Boolean finalWriteSuccessful = writeToHDFS(filePath, writeAttempts, tempFile,cleanupTempFile);
        if (Boolean.FALSE.equals(finalWriteSuccessful)) {
            xenonLoadedFlag = false;
            try {
                throw new IOException("All write attempts to HDFS failed while uploading to " + filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!xenonLoadedFlag) {
            throw new IOException("Write attempts to HDFS failed.Check the logs for the xenon failure ");
        }
    }
}
