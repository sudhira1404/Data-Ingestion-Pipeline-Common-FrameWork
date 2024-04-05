package com.target.kelsaapi.common.service;

import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.service.rest.HttpServiceImpl;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;


@ExtendWith(SpringExtension.class)
@SpringBootTest
public class HttpServiceImplTest {


    @InjectMocks
    HttpServiceImpl httpService;

    @Mock
    @Qualifier("restTemplate")
    RestTemplate restTemplate;


    Map<String,String> map = new HashMap<>();

    @Test
    public void testGet() throws HttpException, HttpRetryableException {

        when(restTemplate.exchange(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.GET),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(String.class)
        )).thenReturn(ResponseEntity.ok(""));
        HttpCustomResponse actualResponse = httpService.get("test", map);

        assertEquals(actualResponse.getStatusCode().intValue(), HttpStatus.OK.value());
    }

    @Test
    public void testPost() throws HttpException {

        when(restTemplate.exchange(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.POST),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(String.class)
        )).thenReturn(ResponseEntity.ok(""));

        HttpCustomResponse actualResponse = httpService.post("test",map,map);

        assertEquals(actualResponse.getStatusCode().intValue(), HttpStatus.OK.value());
    }

    @Test
    public void testOptions() throws HttpException {
        when(restTemplate.exchange(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq(HttpMethod.OPTIONS),
                ArgumentMatchers.any(),
                ArgumentMatchers.eq(String.class)
        )).thenReturn(ResponseEntity.ok(""));

        HttpCustomResponse actualResponse = httpService.options("test",map);

        assertEquals(actualResponse.getStatusCode().intValue(), HttpStatus.OK.value());
    }
}