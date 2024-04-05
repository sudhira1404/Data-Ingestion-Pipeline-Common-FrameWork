package com.target.kelsaapi.common.service.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ApplicationHDFSFileWriterServiceTest {

    @Mock
    HDFSFileWriterService HDFSFileWriterService;

    PipelineConfig.Hdfsdetails hdfsdetails;

    @Mock
    HttpCustomResponse httpCustomResponse;

    @BeforeAll
    public void setUp() throws JsonProcessingException {
        this.hdfsdetails = new PipelineConfig.Hdfsdetails();
        hdfsdetails.setFormat("%s/xenon/fs/bigred3ns/%s");
        hdfsdetails.setEndPoint("https://xenon.bigred3.target.com/xenon/fs");
        hdfsdetails.setPassword("bar");
        hdfsdetails.setUserName("foo");

        when(httpCustomResponse.getBody()).thenReturn("123");
        when(httpCustomResponse.getStatusCode()).thenReturn(Long.valueOf("200"));
        when(httpCustomResponse.getHeaders()).thenReturn(HttpHeaders.EMPTY);
    }

    @Test
    public void testWriteSuccessFailureRecordsToHDFS() {



    }

    public void testSinkSuccessRecords() {



    }

    public void testSinkReAttemptRecords() {
    }

    public void testPushReAttemptMessageForRetryByTime() {
    }

    public void testSinkErrorRecords() {
    }
}