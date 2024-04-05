package com.target.kelsaapi.common.service.google;

import com.target.kelsaapi.common.exceptions.GoogleMarketingPlatformException;
import com.target.kelsaapi.common.service.google.marketingplatform.CampaignManager360Interface;
import com.target.kelsaapi.common.vo.google.request.marketingplatform.CampaignManager360ReportRequest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class CampaignManager360InterfaceImplTest {

    @Mock
    private CampaignManager360Interface service;

    private CampaignManager360ReportRequest request;

    private InputStream testStream;

    @BeforeAll
    public void setUp() throws Exception {

        this.request = new CampaignManager360ReportRequest("2021-04-26", "2021-04-26");

        this.testStream = IOUtils.toInputStream("ABC", StandardCharsets.UTF_8);
    }

    @Test
    public void testGetData() throws IOException, InterruptedException, GoogleMarketingPlatformException {
        when(service.getData(request)).thenReturn(testStream);
        assertEquals(testStream, service.getData(request));
    }
}