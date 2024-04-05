package com.target.kelsaapi.common.service.tradedesk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.vo.Oauth;

import java.util.List;
import java.util.Map;

public interface TradedeskService {
    Map<String, String> setHeaderMap(Oauth oauth);

    String getReportDownloadLink(Map<String, String> headersMap, String startDate, String partnerId, int reportId, String baseUrl, String endpoint) throws HttpException, JsonProcessingException;

    String getReportDownload(Map<String, String> headersMap, String downloadLinkUrl) throws HttpException, HttpRetryableException;

    String getEndPointData(Map<String, String> headersMap, String partnerId, String baseUrl, String endpoint, int startIndex, int pageSize) throws HttpException, JsonProcessingException;

    List<String> getApiDataWithPagination(Map<String, String> headersMap, String partnerId, String baseUrl, String endpoint) throws HttpException, HttpRetryableException, JsonProcessingException;

    List<String> getAllAdvertiserIds(Map<String, String> headersMap, String partnerId, String baseUrl, String endpoint) throws HttpException, HttpRetryableException, JsonProcessingException;
}
