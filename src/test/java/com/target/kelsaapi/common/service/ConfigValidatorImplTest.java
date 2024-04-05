package com.target.kelsaapi.common.service;

import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.config.ConfigValidatorImpl;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ConfigValidatorImplTest {

    @InjectMocks
    ConfigValidatorImpl configValidator;

    @Mock
    PipelineConfig.Source source;

    @Mock
    PipelineConfig.Google google;

    @Mock
    PipelineConfig.Google.MarketingPlatform marketingPlatform;

    @Mock
    PipelineConfig.Google.AdManager adManager;

    @Test
    public void testValidateSourceProcess() throws ConfigurationException {

        when(source.getGoogle()).thenReturn(google);
        when(source.getGoogle().getAdManager()).thenReturn(adManager);
        when(source.getGoogle().getMarketingPlatform()).thenReturn(marketingPlatform);

        assertFalse(configValidator.validateSourceProcess(source));

    }

    @Mock
    PipelineConfig.Hdfsdetails hdfsdetails;

    @Test
    public void testValidateHdfsDetails() {

        when(hdfsdetails.getEndPoint()).thenReturn("test");
        when(hdfsdetails.getFormat()).thenReturn("test");

        assertTrue(configValidator.validateHdfsDetails(hdfsdetails));
    }

    @Mock
    PipelineConfig.Secrets secrets;

    @Test
    public void testValidateSecrets() {

        when(secrets.getFieldname()).thenReturn("test");
        when(secrets.getTapkey()).thenReturn("test");

        assertTrue(configValidator.validateSecrets(Collections.singletonList(secrets)));
    }

}