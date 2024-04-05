package com.target.kelsaapi.common.service.rest;

import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import org.springframework.http.HttpHeaders;

import java.util.Map;

/**
 * Interface having all the http interaction
 *
 * @since 1.0
 */
public interface HttpService {
    HttpCustomResponse get(String endPointURL , Map<String,String> headers) throws HttpException, HttpRetryableException;
    HttpCustomResponse post(String endPointURL , Map<String,String> headersMap , Map<String,String> bodyMap)throws HttpException;
    HttpCustomResponse post(String endPointURL, Map<String, String> headersMap, String body) throws HttpException;
    HttpCustomResponse post(String endPointURL, HttpHeaders headers, Object body) throws HttpException;
    HttpCustomResponse options(String endPointURL , Map<String,String> headers )throws HttpException;

}
