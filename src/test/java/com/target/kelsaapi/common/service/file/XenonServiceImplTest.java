package com.target.kelsaapi.common.service.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.NotFoundException;
import com.target.kelsaapi.common.exceptions.ReaderException;
import com.target.kelsaapi.common.exceptions.WriterException;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class XenonServiceImplTest {

    PipelineConfig.Hdfsdetails hdfsdetails;

    @Mock
    HttpService httpService;

    @Mock
    @Qualifier("restTemplate")
    RestTemplate restTemplate;

    XenonServiceImpl xenonService;

    @BeforeAll
    public void setUp() throws JsonProcessingException {
        this.hdfsdetails = new PipelineConfig.Hdfsdetails();
        hdfsdetails.setFormat("%s/xenon/fs/bigred3ns/%s");
        hdfsdetails.setEndPoint("https://xenon.bigred3.target.com/xenon/fs");
        hdfsdetails.setPassword("bar");
        hdfsdetails.setUserName("foo");

        this.xenonService = new XenonServiceImpl(this.httpService, this.restTemplate, this.hdfsdetails);

        String json = "{ \""+ ApplicationConstants.SERVERS+"\" : [\"https://xenon.bigred3.target.com/xenon/fs\",\"https://xenon.bigred3.target.com/xenon/fs\"] } ";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(json);

        when(restTemplate.exchange(ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.OPTIONS),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(JsonNode.class))).thenReturn(ResponseEntity.ok(jsonNode));
    }



    @Test
    public void testReadFile() throws ReaderException {

        String expectedResult ="123\n345";

        when(restTemplate.exchange(ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.GET),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(String.class))).thenReturn(ResponseEntity.ok(expectedResult));

        List<String> result = xenonService.readFile("/test");

        assertEquals(result.get(0),"123");

    }

    @Test
    public void testDeleteFile() throws WriterException {

        when(restTemplate.execute(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.DELETE),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
        )).thenReturn("");

        Boolean success = xenonService.deleteFile("/test.txt");
        assertTrue(success);
    }

    @Test
    public void testIsFileExists() {

        when(restTemplate.execute(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.GET),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
        )).thenReturn(true);

        Boolean success =  xenonService.isFileExists("/test.txt");

        assertTrue(success);

    }

    @Test
    public void testCreateFolder() throws WriterException {

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.createObjectNode();

        when(restTemplate.exchange(ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.PUT),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(JsonNode.class))).thenReturn(ResponseEntity.ok(jsonNode));

        Boolean success = xenonService.createFolder("/test");

        assertTrue(success);
    }

    @Test
    public void testReadFolder() throws ReaderException {
        List<String> expectedResult = Arrays.asList("123","345");

        when(restTemplate.execute(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.GET),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
        )).thenReturn(expectedResult);

        List<String> actualResult = xenonService.readFolder("/test");

        assertEquals(expectedResult,actualResult);
    }

    @Test
    public void testDeleteFolder() throws WriterException {
        when(restTemplate.execute(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.DELETE),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
        )).thenReturn("");

        Boolean success = xenonService.deleteFolder("/test");
        assertTrue(success);
    }


    @Test
    public void testIsFolderExists() throws NotFoundException {

        when(restTemplate.execute(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.GET),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
        )).thenReturn(true);

        Boolean success =  xenonService.isFolderExists("/test");

        assertTrue(success);
    }

    @Test
    public void testTransferFile() throws URISyntaxException {
        String filePath = "/foo/bar/file.out";
        String tempFile = "temp.out";
        String xenonNode = "brdnc1234.target.com";
        String formatString = hdfsdetails.getFormat() + ApplicationConstants.XENONWRITEPARAM;
        String xenonFormattedAPI = String.format(formatString, xenonNode, filePath, false, true, true);

        URI uri = new URI(xenonFormattedAPI);

        when(restTemplate.exchange(ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.PUT),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(String.class))).thenReturn(ResponseEntity.created(uri).body("Everything is awesome"));

        assertTrue(xenonService.transferFile(filePath, tempFile, false, true, true));
    }
}