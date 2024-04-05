package com.target.kelsaapi.common.service.file;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.UnImplementedException;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class HDFSFileWriterServiceImplTest {

    @Mock
    XenonServiceImpl xenonService;

    PipelineConfig pipelineConfig;

    PipelineConfig.Hdfsdetails hdfsdetails;

    @Mock
    LocalFileWriterService localFileWriter;

    HDFSFileWriterService hdfsFileWriterService;

    @BeforeAll
    public void setUp() {

        //this.localFileWriter = new LocalFileWriterServiceImpl();
        this.pipelineConfig = new PipelineConfig();
        PipelineConfig.Apiconfig apiConfig = new PipelineConfig.Apiconfig();

        this.hdfsdetails = new PipelineConfig.Hdfsdetails();
        hdfsdetails.setFormat("%s/xenon/fs/bigred3ns/%s");
        hdfsdetails.setEndPoint("https://xenon.bigred3.target.com/xenon/fs");
        hdfsdetails.setPassword("bar");
        hdfsdetails.setUserName("foo");

        apiConfig.setHdfsdetails(hdfsdetails);
        pipelineConfig.setApiconfig(apiConfig);

        this.hdfsFileWriterService = new HDFSFileWriterServiceImpl(this.pipelineConfig, this.xenonService, this.localFileWriter);
    }

    @Test
    public void testWriteToHDFSFileExists() {
        String filePath = "/test";
        Integer writeAttempts = 1;
        String tempFile = "/test/tmp.txt";
        Boolean cleanupTempFile = true;

        when(xenonService.isFileExists(filePath)).thenReturn(true);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(filePath, writeAttempts, tempFile, cleanupTempFile));
    }

    @Test
    public void testWriteToHDFSFileNotExists() {
        String filePath = "/test";
        Integer writeAttempts = 1;
        String tempFile = "/test/tmp.txt";
        Boolean cleanupTempFile = true;

        when(xenonService.isFileExists(filePath)).thenReturn(false);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(filePath, writeAttempts, tempFile, cleanupTempFile));
    }

    @Test
    public void testWriteInputStreamToHDFS() throws UnImplementedException {
        InputStream is = new ByteArrayInputStream("ABC".getBytes(StandardCharsets.UTF_8));
        String filePath = "/test";
        Integer writeAttempts = 1;
        String tempFile = "tmp.txt";
        Boolean cleanupTempFile = true;

        when(localFileWriter.retryableLocalWrite(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        when(xenonService.isFileExists(filePath)).thenReturn(true);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(is, filePath, writeAttempts, tempFile, cleanupTempFile));
    }

    @Test
    public void testWriteTarGzipToHDFS() throws UnImplementedException {
        String filePath = "/test";
        String outputGZTarFile = "test.tar.gz";
        List<Path> inputFilePaths = new ArrayList<>();
        Integer writeAttempts = 1;
        Boolean cleanupTempFile = true;

        inputFilePaths.add(Paths.get("/temp.temp.txt"));

        when(localFileWriter.retryableLocalWrite(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        when(xenonService.isFileExists(filePath)).thenReturn(true);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(filePath, outputGZTarFile, inputFilePaths, writeAttempts, cleanupTempFile));
    }

    @Test
    public void testWriteListToHDFS() throws UnImplementedException {
        List<String> contents = new ArrayList<>();
        String filePath = "/test";
        Integer writeAttempts = 1;
        String tempFile = "/test/test.txt";
        Boolean cleanupTempFile = true;

        contents.add("ABC");

        when(localFileWriter.retryableLocalWrite(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        when(xenonService.isFileExists(filePath)).thenReturn(true);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(contents, filePath, writeAttempts, tempFile, cleanupTempFile));
    }


    @Test
    public void testWriteStringToHDFS() throws UnImplementedException {
        String contents = "ABC";
        String filePath = "/test";
        Integer writeAttempts = 1;
        String tempFile = "/test/test.txt";
        Boolean cleanupTempFile = true;

        when(localFileWriter.retryableLocalWrite(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        when(xenonService.isFileExists(filePath)).thenReturn(true);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(contents, filePath, writeAttempts, tempFile, cleanupTempFile));
    }

    @Test
    public void testWriteDefaultsToHDFS() throws UnImplementedException {
        String contents = "ABC";
        String filePath = "/test";
        int DEFAULT_MAX_WRITE_ATTEMPTS = 3;
        String DEFAULT_TEMP_FILE = ApplicationConstants.TEMPFILE;

        when(localFileWriter.retryableLocalWrite(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true);
        when(xenonService.isFileExists(filePath)).thenReturn(true);
        when(xenonService.transferFile(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean(),
                ArgumentMatchers.anyBoolean())).thenReturn(true);
        assertTrue(hdfsFileWriterService.writeToHDFS(contents, filePath));
    }
}