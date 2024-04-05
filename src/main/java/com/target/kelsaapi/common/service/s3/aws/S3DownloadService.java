package com.target.kelsaapi.common.service.s3.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.*;
import com.target.kelsaapi.common.service.file.LocalHDFSFileWriterService;
import com.target.kelsaapi.common.util.textFormatterInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

@Slf4j
@Service
public class S3DownloadService implements textFormatterInterface {

    protected LocalHDFSFileWriterService localHDFSFileWriterService;

    public S3DownloadService(ApplicationContext context) {
        this.localHDFSFileWriterService = context.getBean(LocalHDFSFileWriterService.class);
    }

    public void downloadFileOrDir(AmazonS3 s3Client, String bucketName, String keyName,File tempFileDirectory) {

        log.info("Downloading from S3 bucket=>" + bucketName + " the file => "  + keyName );
        File fileObject = new File(keyName);
        String fileName = fileObject.getName();
        //String directoryPath = FilenameUtils.getPath(fileObject.toString());
        //File directory = new File(directoryPath);
        log.info("Directory name where file will be downloaded locally " + tempFileDirectory);
        if (! tempFileDirectory.exists()){
            tempFileDirectory.mkdirs();
        }
        File tempFilePath = new File(tempFileDirectory.toString() + File.separatorChar + fileName);
        S3Object file = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
        try {
            S3ObjectInputStream s3is = file.getObjectContent();
            FileOutputStream fos = new FileOutputStream(tempFilePath);
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            log.error(ANSI_RED + e.getErrorMessage() + ANSI_RESET);
            //System.exit(1);
        } catch (FileNotFoundException e) {
            log.error(ANSI_RED +e.getMessage() + ANSI_RESET);
            //System.exit(1);
        } catch (IOException e) {
            log.error(ANSI_RED + e.getMessage() + ANSI_RESET);
            //System.exit(1);
        }
    }

    public void downloadDir(AmazonS3 s3Client,String bucketName, String keyName,
                                   boolean pause,File tempFileDirectory) {

        log.info("Downloading from S3 bucket=>" + bucketName + " the file => "  + keyName );
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        try {
            MultipleFileDownload xfer = xfer_mgr.downloadDirectory(bucketName, keyName, tempFileDirectory);
            S3DownloadProgressService.showTransferProgress(xfer);
            S3DownloadProgressService.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            log.error(ANSI_RED + e.getErrorMessage() + ANSI_RESET);
            //System.exit(1);
        }
        xfer_mgr.shutdownNow();


    }

    public void downloadFile(AmazonS3 s3Client,String bucketName, String keyName,
                                    boolean pause,File tempFileDirectory) {

        log.info("Downloading from S3 bucket=>" + bucketName + " the file => "  + keyName );
        File fileObject = new File(keyName);
        String fileName = fileObject.getName();
        File tempFilePath = new File(tempFileDirectory.toString() + File.separatorChar + fileName);
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        try {
            Download xfer = xfer_mgr.download(bucketName, keyName, tempFilePath);
            S3DownloadProgressService.showTransferProgress(xfer);
            S3DownloadProgressService.waitForCompletion(xfer);

        } catch (AmazonServiceException e) {
            log.error(ANSI_RED +  e.getErrorMessage() + ANSI_RESET);
            //System.exit(1);
        }
        xfer_mgr.shutdownNow();

    }

    public static void downloadFileWithListener(AmazonS3 s3Client,
                                                String bucketName, String keyName, boolean pause,File tempFileDirectory) {

        log.info("Downloading from S3 bucket=>" + bucketName + " the file => "  + keyName );
        S3Object file = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
        File fileObject = new File(file.getKey());
        String fileName = fileObject.getName();
        File tempFilePath = new File(tempFileDirectory.toString() + File.separatorChar + fileName);
        log.info("Downloading to the file: " + fileObject);
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        try {
            Download u = xfer_mgr.download(bucketName, keyName, tempFilePath);
            S3DownloadProgressService.printProgressBar(0.0);
            u.addProgressListener(new ProgressListener() {
                public void progressChanged(ProgressEvent e) {
                    double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
                    S3DownloadProgressService.eraseProgressBar();
                    S3DownloadProgressService.printProgressBar(pct);
                }
            });
            S3DownloadProgressService.waitForCompletion(u);
            Transfer.TransferState xfer_state = u.getState();
            log.info("final state of the transfer: " + xfer_state);
        } catch (AmazonServiceException e) {
            log.error(ANSI_RED + e.getErrorMessage() + ANSI_RESET);
            //System.exit(1);
        }
        xfer_mgr.shutdownNow();
    }

    public void  downloadFileFromUrlMoveToHDFS(AmazonS3 s3Client, String bucketName, String keyName, File tempFileDirectory, String targetFilePath,
                                               Boolean splitFileCompress,
                                               Integer maxLinesBeforeFlush, Long maxSizeBeforeNewFile
    ) throws IOException {


        log.info("Downloading from S3 bucket=>" + bucketName + " the file => "  + keyName );
        File fileObject = new File(keyName);
        String fileName = fileObject.getName();
        String fileNameWithOutExt = fileName;
        String fileExt="";
        int dotIndexFilename= fileName.indexOf(".");
        if (dotIndexFilename != -1)
        {
            fileNameWithOutExt= fileName.substring(0 , dotIndexFilename);
            fileExt= fileName.substring(dotIndexFilename);
        }

        URL url = generateUrl( s3Client,  bucketName,  keyName);
        log.info("Directory name where file will be downloaded locally " + tempFileDirectory);
        if (! tempFileDirectory.exists()){
            tempFileDirectory.mkdirs();
        }

        //using url as inputstream
        localHDFSFileWriterService.writeLocalFileToHdfs( url, fileNameWithOutExt,
                fileExt, tempFileDirectory.toString(),targetFilePath,splitFileCompress,maxLinesBeforeFlush,maxSizeBeforeNewFile);

    }


    public  URL generateUrl(AmazonS3 s3Client, String bucketName, String keyName) {

        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 604800 * 1000; //7 days
        expiration.setTime(expTimeMillis);
        URL url = s3Client.generatePresignedUrl(bucketName, keyName, expiration, HttpMethod.GET);
        String s3Url = s3Client.getUrl(bucketName, keyName).toString();
        log.debug("URL to download from : {}" + url);
        log.info("URL to download from : {}", s3Url);
        return url;
    }

}
