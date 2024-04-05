package com.target.kelsaapi.common.service.salesforce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.postgres.salesforce.SalesforceStateRepository;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.salesforce.response.SalesforceResponse;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ContentType;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@Service
public class SalesforceServiceImpl implements SalesforceService {

    private final HttpService httpService;

    private final PipelineConfig.Salesforce salesforceConfig;

    private final LocalFileWriterService localFileWriterService;

    private final SalesforceStateRepository salesforceStateRepository;

    @Autowired
    public SalesforceServiceImpl(HttpService httpService, PipelineConfig config, LocalFileWriterService localFileWriterService, SalesforceStateRepository salesforceStateRepository) {
        this.httpService = httpService;
        this.salesforceConfig = config.getApiconfig().getSource().getSalesforce();
        this.localFileWriterService = localFileWriterService;
        this.salesforceStateRepository = salesforceStateRepository;
    }

    @Override
    public List<String> getSalesforceObjectData(Oauth oauth, String pipelineRunId, String reportType) throws IOException {

        MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME,pipelineRunId);

        //Initialize header
        Map<String,String> headersMap = setHeaderMap(oauth);

        List<String> finalList = new ArrayList<>();

            try {

                List<String> results  = getSalesforceObjectData(reportType, headersMap);

                if (results.size() > 0) {
                    log.info("Result added to final list");
                    finalList = Stream.concat(finalList.stream(), results.stream()).toList();
                }
                else {
                    log.info("Result is empty");
                }
            } catch (HttpException | JsonProcessingException | RuntimeException | HttpRetryableException e) {
                log.error(e.getMessage(), e.getCause());
            } catch (InterruptedException ie) {
                log.error(ie.getMessage(),ie.getCause());
            }
        if (!finalList.isEmpty()) {
            log.info("Total count of salesforce result set is " + finalList.size());
            return finalList;
        } else {
            throw new RuntimeException("There were no results for salesforce object!");
        }
    }

    private List<String> getSalesforceObjectData(String reportType, Map<String, String> headersMap) throws HttpException, JsonProcessingException, HttpRetryableException, InterruptedException {

        String nextRecordsUrl;

        List<String> resultList = new ArrayList<>();
        {
            String url = buildBaseUrl(reportType);
            HttpCustomResponse serviceResponse = httpService.get(url, headersMap);
            ObjectMapper mapper = new ObjectMapper();
            SalesforceResponse salesforceData = mapper.readValue(serviceResponse.getBody(), SalesforceResponse.class);
            nextRecordsUrl = salesforceData.getNextRecordsUrl();
            log.debug("nextRecordsUrl is : " + salesforceData.getNextRecordsUrl());
            resultList.add(serviceResponse.getBody());
        }

            while (nextRecordsUrl != null) {
                String url = buildBaseUrlWithNextRecordsUrl(nextRecordsUrl, reportType);
                HttpCustomResponse serviceResponse = httpService.get(url, headersMap);
                ObjectMapper mapper = new ObjectMapper();
                SalesforceResponse salesforceData = mapper.readValue(serviceResponse.getBody(), SalesforceResponse.class);
                nextRecordsUrl = salesforceData.getNextRecordsUrl();
                log.debug("nextRecordsUrl is : " + salesforceData.getNextRecordsUrl());
                resultList.add(serviceResponse.getBody());
            }

         return resultList;
    }

    private Map<String, String> setHeaderMap(Oauth oauth) {
        Map<String,String> headersMap = new LinkedHashMap<>();
        headersMap.put("Authorization","Bearer " + oauth.getOAuthToken());
        headersMap.put("Accept", MediaType.APPLICATION_JSON_VALUE);
        headersMap.put("Content-Type", ContentType.APPLICATION_JSON.toString());
        return headersMap;
    }


    private String buildBaseUrl(String reportType) {
        String urlPrefix = buildUrlPrefix() + "/services/data/v58.0/query/";
        String query = buildQuery(reportType);
        String startingUrl = urlPrefix + query;
        log.info("Final URL to decorate: {}",startingUrl);
        return startingUrl;
    }

    private String buildQuery(String reportType) {

        List<String> attributes = salesforceStateRepository.getSalesforceAttributesByReportType(reportType);
        System.out.println("attributes: " + attributes);
        //String attributes = "Name,NumberOfEmployees";
        String query = "?" + "q=" + "SELECT " + attributes.get(0) + " from "+ reportType;
        log.info("Final query: {}",query);
        return query;
    }


    private String buildBaseUrlWithNextRecordsUrl(String nextRecordsUrl, String reportType) {
        String urlPrefix = buildUrlPrefix() + nextRecordsUrl;
        String query = buildQuery(reportType);
        String startingUrl = urlPrefix + query;
        log.info("Final URL to decorate: {}",startingUrl);
        return startingUrl;
    }

    private String buildUrlPrefix() {
        String urlPrefix = salesforceConfig.baseUrl + "/";

        log.info("URL base from configs: {}", urlPrefix);
        return urlPrefix;
    }

}
