package com.target.kelsaapi.common.service.file;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.google.api.client.util.Lists;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Implementation of LocalHDFSFileWriterService that contains all methods pertaining to local to hdfs file writing.
 */
@Slf4j
@Service
public class LocalHDFSFileWriterServiceImpl implements LocalHDFSFileWriterService{

    protected HDFSFileWriterService writerService;

    protected PipelineConfig pipelineConfig;

    protected LocalFileWriterService localFileWriterService;

    @Autowired
    public LocalHDFSFileWriterServiceImpl(ApplicationContext context) {

        this.writerService = context.getBean(HDFSFileWriterService.class);
        this.pipelineConfig = context.getBean(PipelineConfig.class);
        this.localFileWriterService = context.getBean(LocalFileWriterService.class);
    }


//add retry
    /**
     * Streaming writer from URL(input format could be gz or any other format) to local  gzipped or no gzip file reading each line of the input source.
     * The stream is read into smaller lists, and when the  maximum lines configured for flush have been reached,
     * contents are flushed to a raw file. A new raw file is created after the maximum lines configured for new file
     * is reached. The first record from the stream is considered the header and will be repeated at the beginning of
     * each new raw file. Once all lines from the stream have been read, all raw files will be moved into a final
     * tar file and gzipped locally.
     *
     * @param downloadURL The {@link URL} to open the stream against and read lines.
     * @param localFilePathNoExtension The root of the file path used for all raw files and the final tar file.
     * @param localFileExtension The extension of the raw files, typically ".csv"
     * @param tempFileDirectory The directory where file will be downloaded locally"
     * @param targetFilePath The HDFS file path where downloaded file will be moved"
     * @param compress True to compress the raw files before moving them to the tar file; false to only gzip the tar file.
     * @param maxLinesBeforeFlush Sets the maximum size of the List object used to buffer the stream before flushing to a raw file.
     * @param maxSizeBeforeNewFile Sets the maximum size each raw file can grow before a new file will be started.
     * @return If download or xenon move failed then throw exception and stop the flow.
     */
    @Override
    public void writeLocalFileToHdfs(URL downloadURL, String localFilePathNoExtension,
                                     String localFileExtension,String tempFileDirectory,String targetFilePath, Boolean compress,
                                     @Nullable Integer maxLinesBeforeFlush, @Nullable Long maxSizeBeforeNewFile
    ) throws IOException {

        Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;
        //if (maxLinesBeforeFlush == null) maxLinesBeforeFlush = 100000;
        //if (maxSizeBeforeNewFile == null) maxSizeBeforeNewFile = 1000000000L;
        int flush = 1;
        int newFile = 1;
        int newFileLineCounter = 1;
        BufferedReader bufferedReader;
        BufferedInputStream  bufferedInputStream;
        String targetFilePathNew;
        String fileName = localFilePathNoExtension + "_" + newFile + localFileExtension;
        String fileNamePath = tempFileDirectory + "/" + fileName;
        File f = new File(fileNamePath);
        long fileSize = f.length();
        HttpsURLConnection httpConn = null;
        SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        log.info(" File that will be downloaded to local " + fileNamePath);

        if (targetFilePath.lastIndexOf('/') == targetFilePath.length() - 1) {
            targetFilePathNew = targetFilePath;
        } else {
            targetFilePathNew = targetFilePath + File.separator;
        }

        String targetFileNamePath = targetFilePathNew + fileName;
        log.info("Hdfs path where files will be moved  " + targetFileNamePath);

        try  {

            URLConnection con = downloadURL.openConnection();
            con.setConnectTimeout(3600*1000);
            con.setReadTimeout(3600*1000);
            InputStream inputStream = con.getInputStream();
            Scanner scanner = new Scanner(inputStream);
            if (localFileExtension.contains(".gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));
            } else {
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                bufferedReader = new BufferedReader(inputStreamReader);
            }
            List<String> lines = null;
            List<Path> files = Lists.newArrayList();
            String line;
            String header = bufferedReader.readLine();
            log.debug("Header : {}", header);
            while ((line = bufferedReader.readLine()) != null) {
                if (flush == 1) {
                    lines = Lists.newArrayList();
                    if (newFileLineCounter == 1) {
                        log.debug("Adding Header to the beginning of a new file");
                        lines = Lists.newArrayList();
                        lines.add(header);
                    }
                }
                lines.add(line);
                flush++;
                newFileLineCounter++;
                f = new File(fileNamePath);
                fileSize = f.length();
                if (fileSize >= maxSizeBeforeNewFile) {
                    Boolean writeCheck;
                    if (compress) {
                        writeCheck = writeListGzipFile(lines, fileNamePath, targetFileNamePath, true, true);
                    } else {
                        writeCheck = writeListFile(lines, fileNamePath, targetFileNamePath, true, true);
                    }
                    if (writeCheck) {
                        log.info("Successfully wrote {} lines to file {} and moved to hdfs path", newFileLineCounter, targetFileNamePath);
                        files.add(Paths.get(fileName));
                        flush = 1;
                        newFileLineCounter = 1;
                        newFile++;
                        fileName = localFilePathNoExtension + "_" + newFile + localFileExtension;
                        fileNamePath = tempFileDirectory + "/" + fileName;
                        targetFileNamePath = targetFilePathNew + fileName;
                        lines = Lists.newArrayList();//added so to avoid final flush to insert again if final record matches maxSizeBeforeNewFile
                        log.info("Maximum file size {} reached, will begin buffered writing to new file {} on the next line read",
                                fileSize, fileNamePath);
                        f = new File(fileNamePath);
                        fileSize = f.length();
                    } else {
                        throw new IOException("Failed on final write to file path " + fileNamePath);
                    }
                }

                if (maxLinesBeforeFlush.equals(flush)) {
                    Boolean writeCheck;
                    if (compress) {
                        writeCheck = writeListGzipFile(lines, fileNamePath, targetFileNamePath, true, false);

                    } else {
                        writeCheck = writeListFile(lines, fileNamePath, targetFileNamePath, true, false);
                    }
                    if (writeCheck) {
                        //log.debug("Successfully flushed {} lines to file path {}", flush, fileNamePath);
                        flush = 1;
                    } else {
                        throw new IOException("Failed to append line buffer to file path " + fileNamePath);
                    }
                }
            }

            inputStream.close();
            bufferedReader.close();
            scanner.close();

            if (lines != null && !lines.isEmpty()) {
                Boolean writeCheck;
                if (compress) {
                    writeCheck = writeListGzipFile(lines, fileNamePath, targetFileNamePath, true, true);

                } else {
                    writeCheck = writeListFile(lines, fileNamePath, targetFileNamePath, true, true);
                }
                if (writeCheck) {
                    log.info("Successfully flushed final {} lines to file {} and moved to hdfs path {}", flush, fileNamePath, targetFileNamePath);
                    files.add(Paths.get(fileName));
                } else {
                    throw new IOException("Failed to append final line buffer to file " + fileName);
                }
            }

            log.info("Original file {} is split into {}  local files {} and moved to hdfs path {}", localFilePathNoExtension + localFileExtension, files.size(), files.toString(), targetFileNamePath);

        } catch (AmazonServiceException e) {
            log.error(e.getMessage(), e.getCause());
            throw new AmazonServiceException("AmazonServiceException");
        } catch (SdkClientException e) {
            e.printStackTrace();
            log.error(e.getStackTrace()[0].toString());
            throw new SdkClientException("SdkClientException");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error(e.getStackTrace()[0].toString());
            log.error(e.getMessage());
            throw new FileNotFoundException("FileNotFoundException");
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getStackTrace()[0].toString());
            log.error(e.getMessage());
            throw new IOException("IOException");
        }
    }


    /**
     * Uses {@link GzipCompressorOutputStream} to write to a local file and optional move to hdfs .Adds a newline character at the end of each
     * String in the List.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @param targetFileNamePath The file to write to hdfs.
     * @param HdfsMove Boolean to move to hdfs or not.
     * @return True if successful; False if unsuccessful.
     */

    private Boolean writeListGzipFile(List<String> contents, String localFilePath, String targetFileNamePath,Boolean append,Boolean HdfsMove) throws IOException {

        try (FileOutputStream output = new FileOutputStream(localFilePath, append);
             Writer bout = new OutputStreamWriter(new GzipCompressorOutputStream(output))) {
            for (String s : contents) {
                s += System.getProperty("line.separator");
                bout.write(s);
            }
            bout.flush();
            bout.close();
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Unable to write to local temp file : " + localFilePath);
            log.error(e.getMessage());
            return false;
        }

        if (HdfsMove) {
            log.info(" Start the HDFS move of the file {} and delete from local after move {}", targetFileNamePath, localFilePath);
            writerService.writeToHDFS( targetFileNamePath,  localFilePath,3,  true);
            //writerService.writeToHDFSHttp( targetFileNamePath,  localFilePath,  true);

            log.info("Successfully moved file {} to hdfs path {} and deleted from local", localFilePath, targetFileNamePath);
        }

        return true;
    }

    /**
     * Uses {@link BufferedOutputStream} to write to a local file and optional move to hdfs. Adds a newline character at the end of each String
     * in the List.
     *
     * @param contents The List of Strings to write.
     * @param localFilePath The file to write to.
     * @param targetFileNamePath The file to write to hdfs.
     * @param HdfsMove Boolean to move to hdfs or not.
     * @return True if successful; False if unsuccessful.
     */
    private Boolean writeListFile(List<String> contents, String localFilePath, String targetFileNamePath,Boolean append,Boolean HdfsMove) throws IOException {

        try (BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(localFilePath, append));) {
            for (String s : contents) {
                s += System.getProperty("line.separator");
                bout.write(s.getBytes());
            }
        } catch (IOException e) {
            log.error("Unable to write to local temp file : " + localFilePath);
            log.error(e.getMessage());
            return false;
        }

        if (HdfsMove) {
            log.info(" Start the HDFS move of the file {} and delete from local after move {}", targetFileNamePath, localFilePath);
            writerService.writeToHDFS( targetFileNamePath,  localFilePath, 3, true);
            log.info("Successfully moved file {} to hdfs path and deleted from local", localFilePath, targetFileNamePath);
        }
        return true;
    }


}
