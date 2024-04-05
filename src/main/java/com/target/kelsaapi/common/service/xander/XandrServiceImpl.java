package com.target.kelsaapi.common.service.xander;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.xandr.Report;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service("xandrService")
public class XandrServiceImpl implements XandrService{
    private final HttpService httpService;

    ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public XandrServiceImpl(HttpService httpService, LocalFileWriterService localFileWriterService) {
        this.httpService = httpService;
    }

    @Override
    public Map<String, String> setHeaderMap(Oauth oauth) {

        Map<String, String> headersMap = new LinkedHashMap<>();
        headersMap.put("Authorization", oauth.getOAuthToken());
        headersMap.put("Accept", MediaType.APPLICATION_JSON_VALUE);
        headersMap.put("Content-Type", ContentType.APPLICATION_JSON.toString());
        return headersMap;
    }

    @Override
    public JsonNode setBodyMap(Report report, String reportType) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode reportNode = mapper.valueToTree(report);
        String[] columns = report.getColumns().get(reportType.toLowerCase());
        ArrayNode array = mapper.valueToTree(columns);
        reportNode.put("report_type", report.getReport_type().get(reportType.toLowerCase()));
        reportNode.put("report_interval", report.getReport_interval().get(reportType.toLowerCase()));
        reportNode.putArray("columns").addAll(array);

        return mapper.createObjectNode().set("report", reportNode);
    }

    @Override
    public String getReportStatus(Map<String, String> headersMap, String baseUrl, String endpoint, String reportId) throws HttpException, HttpRetryableException {
        String url = baseUrl+endpoint+"?id="+reportId;
        log.info("Xandr - report status url: "+url);
        HttpCustomResponse response = httpService.get(url, headersMap);
        String report_status = null;

        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Xandr - Successfully retrieved recent report runs to download.");
            log.debug("Xandr - {}", response.getBody());
            try {

                JsonNode jsonNode = mapper.readTree(response.getBody());
                log.info(jsonNode.toPrettyString());
                report_status = jsonNode.at("/response/execution_status").asText();

            } catch (JsonProcessingException  e) {
                log.error("Xandr - Failed in parsing response: {}", response.getBody());
            }
        }
        return report_status;
    }

    @Override
    public String requestReport(Map<String, String> headersMap, JsonNode bodyMap, String baseUrl, String endpoint) throws HttpException, JsonProcessingException {
        log.debug("Xandr baseUrl: " + baseUrl);
        log.debug("Xandr endpoint: " + endpoint);

        HttpCustomResponse response = httpService.post(baseUrl+endpoint, headersMap, mapper.writeValueAsString(bodyMap));
        String report_id = null;
        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Successfully requested report to download");
            try {
                JsonNode result = mapper.readTree(response.getBody());
                report_id = result.get("response").get("report_id").asText();
            } catch (JsonProcessingException  e) {
                log.error("Failed in parsing response: {}", response.getBody());
            }
        }
        return report_id;
    }

    @Override
    public String downloadReport(Map<String, String> headersMap, String baseUrl, String endpoint, String reportId) throws HttpException, HttpRetryableException {

        String url = baseUrl+endpoint+"?id="+reportId;
        log.info("Xandr - report download url: "+url);
        HttpCustomResponse response = httpService.get(url, headersMap);
        String data = null;
        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {

            log.info("Xandr - Successfully retrieved recent report runs to download.");
            data = response.getBody();
        }else {

            log.error("Xandr - http response status code: {}", response.getStatusCode().toString());
            log.error(response.getBody());
        }
        return data;
    }

}