package com.target.kelsaapi.common.service.tradedesk;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.exceptions.NotFoundException;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.tradedesk.Advertiser;
import com.target.kelsaapi.common.vo.tradedesk.ReportDownloadLink;
import com.target.kelsaapi.common.vo.tradedesk.ReportResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("tradedeskService")
public class TradedeskServiceImpl implements TradedeskService {

    private final HttpService httpService;

    @Autowired
    public TradedeskServiceImpl(HttpService httpService) {
        this.httpService = httpService;
    }

    @Override
    public Map<String, String> setHeaderMap(Oauth oauth) {

        Map<String, String> headersMap = new LinkedHashMap<>();
        headersMap.put("TTD-Auth", oauth.getOAuthToken());
        headersMap.put("Accept", MediaType.APPLICATION_JSON_VALUE);
        headersMap.put("Content-Type", ContentType.APPLICATION_JSON.toString());
        return headersMap;
    }

    @Override
    public String getReportDownloadLink(Map<String, String> headersMap, String startDate, String partnerId, int reportId, String baseUrl, String endpoint) throws HttpException, JsonProcessingException {
        log.debug("Tradedesk baseUrl: " + baseUrl);
        log.debug("Tradedesk endpoint: " + endpoint);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode bodyMap = mapper.createObjectNode();
        bodyMap.set("PartnerIds", mapper.createArrayNode().add(partnerId));
        bodyMap.set("ExecutionStates", mapper.createArrayNode().add("Complete"));
        bodyMap.set("ReportScheduleIds", mapper.createArrayNode().add(reportId));
        bodyMap.put("ExecutionSpansStartDate", startDate);
        bodyMap.put("PageStartIndex", "0");
        bodyMap.put("PageSize", "100");

        HttpCustomResponse response = httpService.post(baseUrl+endpoint, headersMap, mapper.writeValueAsString(bodyMap));
        String downloadUrl = null;
        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Successfully retrieved recent report runs to download.");
            try {
                downloadUrl = parseReportDownloadResponse(response.getBody(), startDate);
            } catch (JsonProcessingException | NotFoundException e) {
                log.error("Failed in parsing response: {}", response.getBody());
            }
        }
        return downloadUrl;
    }

    private String parseReportDownloadResponse(String response, String startDate) throws JsonProcessingException, NotFoundException {

        ObjectMapper mapper = new ObjectMapper();
        ReportDownloadLink res = mapper.readValue(response, ReportDownloadLink.class);
        LocalDate inputDate = LocalDate.parse(startDate);
        List<ReportResult> results = res.getResult();
        log.info("Checking {} reports to see if any are completed and match date {}", results.size(), inputDate);

        for (int i = 0; i <= results.size(); i++) {
            ReportResult result = results.get(i);
            String status = result.getReportExecutionState();
            LocalDate resultDate = LocalDateTime.parse(result.getReportStartDateInclusive()).toLocalDate();

            log.info("Checking result {}", i);
            log.info("ReportExecutionId: {}", result.getReportExecutionId());
            log.info("ReportExecutionState: {}", status);
            log.debug("ReportScheduleId: {}", result.getReportScheduleId());
            log.debug("ReportScheduleName: {}", result.getReportScheduleName());
            log.info("ReportStartDate: {}", resultDate);

            if (result.reportExecutionState.equals("Complete") && inputDate.equals(resultDate)) {
                log.info("Report is completed and matches expected date");
                String returnUrl = result.reportDeliveries.get(0).downloadURL;
                log.debug("Report download URL: {}", result.reportDeliveries.get(0).downloadURL);
                return returnUrl;
            } else {
                log.warn("Report either is not completed or does not match expected date");
            }
        }
        throw new NotFoundException("Unable to find a completed report for the specified date");
    }

    @Override
    public String getReportDownload(Map<String, String> headersMap, String downloadLinkUrl) throws HttpException, HttpRetryableException {
        String reportContents = null;
        HttpCustomResponse response = httpService.get(downloadLinkUrl, headersMap);
        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Success response while getting the downloadable report contents");
            reportContents = response.getBody();
        }
        return reportContents;
    }

    @Override
    public List<String> getApiDataWithPagination(Map<String, String> headersMap, String partnerId, String baseUrl, String endpoint) throws HttpException, JsonProcessingException {

        int startIndex = 0;
        int pageSize = 1000;
        int totalFilteredRecords = 0;
        List<JsonNode> resultListInJsonNode = new ArrayList<>();
        List<String> resListInString = new ArrayList<>();
        String apiData;
        ObjectMapper objectMapper = new ObjectMapper();

        do {
            apiData = getEndPointData(headersMap, partnerId, baseUrl, endpoint, startIndex, pageSize);


            JsonNode jsonNode = objectMapper.readTree(apiData);
            int resultCount = jsonNode.get("ResultCount").asInt();
            totalFilteredRecords = jsonNode.get("TotalFilteredCount").asInt();
            int totalUnfilteredCount = jsonNode.get("TotalUnfilteredCount").asInt();
            JsonNode resultJsonNode = jsonNode.get("Result");

            for(JsonNode node: resultJsonNode){
                resultListInJsonNode.add(node);
                resListInString.add(node.toString());
            }
            startIndex = startIndex + resultCount + 1;
            log.info("startIndex: {}, PageSize: {}, totalFilteredRecords: {}, totalUnfilteredCount: {}", startIndex, pageSize, totalFilteredRecords, totalUnfilteredCount);
        } while (startIndex < totalFilteredRecords);

        log.info("Extracted complete data from {}, for Adevertiser ID: {} ", baseUrl+endpoint, partnerId);
        return resListInString;
    }

    @Override
    public List<String> getAllAdvertiserIds(Map<String, String> headersMap, String advertiserId, String baseUrl, String endpoint ) throws HttpException, JsonProcessingException {

        List<String> data = getApiDataWithPagination(headersMap, advertiserId, baseUrl, endpoint);
        List<String> advertiserIds = new ArrayList<>();
        for (String advertiserData : data) {
            advertiserIds.addAll(parseAdvertiserData(advertiserData));
        }
        return advertiserIds;
    }

    public List<String> parseAdvertiserData(String advertiserData) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        Advertiser advertiserEndPointResult = mapper.readValue(advertiserData, Advertiser.class);
        ArrayList<String> ls = new ArrayList<String>();
        ls.add(advertiserEndPointResult.getAdvertiserId());
        return ls;
    }


    @Override
    public String getEndPointData(Map<String, String> headersMap, String partnerId, String baseUrl, String endpoint, int startIndex, int pageSize) throws HttpException, JsonProcessingException {
        log.debug("Tradedesk api baseURL: {} ", baseUrl);
        log.debug("Tradedesk api endpoint: {}", endpoint);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode bodyMap = mapper.createObjectNode();
        if (endpoint.contains("adgroup")) {
            bodyMap.put("AdvertiserId", partnerId);
        } else {
            bodyMap.put("PartnerId", partnerId);
        }
        bodyMap.put("PageStartIndex", startIndex);
        bodyMap.put("PageSize", pageSize);

        HttpCustomResponse response = httpService.post(baseUrl+endpoint, headersMap, mapper.writeValueAsString(bodyMap));

        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Successfully retrieved data from api");
            return response.getBody();
        }
        log.info("Api response is not 200");
        log.debug(response.getStatusCode().toString());
        return "";
    }
}