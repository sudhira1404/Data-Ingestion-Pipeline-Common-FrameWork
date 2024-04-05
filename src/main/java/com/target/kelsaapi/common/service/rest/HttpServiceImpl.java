package com.target.kelsaapi.common.service.rest;

import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.*;

import java.util.Map;

/**
 * Service for invoking http requests
 */
@Service("httpService")
@Slf4j
public class HttpServiceImpl implements HttpService {

    private final RestTemplate restTemplate;

    @Autowired
    public HttpServiceImpl(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * For making GET request to endpoint
     *
     * @param endPointURL
     * @param headersMap
     * @return
     * @throws HttpException
     */
    @Override
    @Retryable(value = HttpException.class)
    public HttpCustomResponse get(String endPointURL, Map<String, String> headersMap) throws HttpException, HttpRetryableException {
        HttpCustomResponse response = null;
        log.info("Get request is getting executed");
        try {
            //set the headers
            final HttpHeaders headers = CommonUtils.createHeaders(headersMap);
            log.debug("Headers are : " + headers);
            //Create a new HttpEntity
            final HttpEntity<String> entity = new HttpEntity<String>(headers);
            //invoke the endpoint
            ResponseEntity<String> responseEntity = restTemplate.exchange(endPointURL, HttpMethod.GET, entity, String.class);
            response = new HttpCustomResponse(responseEntity.getHeaders(),
                    responseEntity.getBody(),
                    (long) responseEntity.getStatusCode().value());

            log.info("response from api {} for the endpoint {} " , responseEntity.getStatusCode().toString() , endPointURL);
        } catch (HttpClientErrorException | UnknownHttpStatusCodeException restException) {
            log.error("Error in making http request for the url "+endPointURL, restException);
            throw new HttpException(restException.getMessage(), restException.getCause());
        } catch (HttpServerErrorException retryable) {
            throw new HttpRetryableException("Server error encountered! Will retry");
        }
        return response;
    }

    @Override
    @Retryable(value = HttpException.class)
    public HttpCustomResponse post(String endPointURL,
                                   Map<String, String> headersMap,
                                   Map<String, String> bodyMap) throws HttpException {
            //set the headers
            final HttpHeaders headers = CommonUtils.createHeaders(headersMap);
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            //Create body
            final MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
            for (Map.Entry<String, String> bod : bodyMap.entrySet()) {
                body.set(bod.getKey(), bod.getValue());
            }
        return post(endPointURL, headers, body);
    }

    @Override
    @Retryable(value = HttpException.class)
    public HttpCustomResponse post(String endPointURL,
                                   Map<String, String> headersMap,
                                   String body) throws HttpException {
        final HttpHeaders headers = CommonUtils.createHeaders(headersMap);
        return post(endPointURL, headers, body);
    }

    @Override
    @Retryable(value = HttpException.class)
    public HttpCustomResponse post(String endPointURL,
                                   HttpHeaders headers,
                                   Object body) throws HttpException {
        HttpCustomResponse response = null;
        log.info("Post request is getting executed");
        try {
            log.debug("Headers are : " + headers.toString());
            log.debug("Request body is : " + body.toString());
            //Create a new HttpEntity
            HttpEntity<?> entity = new HttpEntity<>(body, headers);
            //invoke the endpoint
            ResponseEntity<String> responseEntity = restTemplate.exchange(endPointURL, HttpMethod.POST, entity, String.class);
            response = new HttpCustomResponse(responseEntity.getHeaders(), responseEntity.getBody(), (long) responseEntity.getStatusCode().value());
            log.info("response from api : " + responseEntity.getStatusCode().toString());
        } catch (RestClientException restException) {
            log.error("Error in making http request", restException);
            throw new HttpException(restException);
        }
        return response;
    }

    @Override
    @Retryable(value = HttpException.class)
    public HttpCustomResponse options(String endPointURL, Map<String, String> headersMap) throws HttpException {
        HttpCustomResponse response = null;
        log.info("Options request getting executed");
        try {
            final HttpHeaders headers = CommonUtils.createHeaders(headersMap);
            //Create a new HttpEntity
            final HttpEntity<String> entity = new HttpEntity<String>(headers);
            //invoke the endpoint
            ResponseEntity<String> responseEntity = restTemplate.exchange(endPointURL, HttpMethod.OPTIONS, entity, String.class);
            response = new HttpCustomResponse(responseEntity.getHeaders(), responseEntity.getBody(), (long) responseEntity.getStatusCode().value());
            log.info("response from api : " + responseEntity.getStatusCode().toString());
        } catch (RestClientException restException) {
            log.error("Error in making http request", restException);
            throw new HttpException(restException);
        }
        return response;

    }
}
