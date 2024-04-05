package com.target.kelsaapi.common.service.xander;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.xandr.Report;

import java.util.Map;

public interface XandrService {

    Map<String, String> setHeaderMap(Oauth oauth);

    String requestReport(Map<String, String> headersMap, JsonNode bodyMap, String baseUrl, String endpoint) throws HttpException, JsonProcessingException;

    String getReportStatus(Map<String, String> headersMap, String baseUrl, String endpoint, String reportId) throws HttpException, HttpRetryableException;

    String downloadReport(Map<String, String> headersMap, String baseUrl, String endpoint, String reportId) throws HttpException, HttpRetryableException ;

    JsonNode setBodyMap(Report report, String reportType) throws JsonProcessingException;

}
