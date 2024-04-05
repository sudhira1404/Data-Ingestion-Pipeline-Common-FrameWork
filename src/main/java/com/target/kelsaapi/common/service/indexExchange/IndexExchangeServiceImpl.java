package com.target.kelsaapi.common.service.indexExchange;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.indexexchange.ReportsList;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ContentType;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service("indexExchangeServiceImpl")
public class IndexExchangeServiceImpl implements IndexExchangeService {

    private final HttpService httpService;
    private final LocalFileWriterService localFileWriterService;
    ObjectMapper mapper = new ObjectMapper();

    private final PipelineConfig.IndexExchange indexExchangeConfig;

    public IndexExchangeServiceImpl(HttpService httpService, LocalFileWriterService localFileWriterService, PipelineConfig config) {
        this.httpService = httpService;
        this.localFileWriterService = localFileWriterService;
        this.indexExchangeConfig = config.apiconfig.source.indexExchange;
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
    public  ArrayList<ReportsList> listReports(Map<String, String> headersMap, int reportId)
            throws HttpException, HttpRetryableException {

        String completeUrl = buildListReportsUrl(reportId);
        log.debug("Index Exchange List Reports from api URL: {}", completeUrl);
        HttpCustomResponse response = httpService.get(completeUrl, headersMap);

        ArrayList<ReportsList> reportsList = new ArrayList<>();
        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Successfully requested List of available Reports with status NEW");
            try {
                ReportsList[] jsonArrList = mapper.readValue(response.getBody(), ReportsList[].class);
                reportsList.addAll(Arrays.asList(jsonArrList));
//                JSONArray jsonArr = mapper.createArrayNode(jsonArrStr);
//                report_id = result.get("response").get("report_id").asText();
            } catch (JsonProcessingException  e) {
                log.error("Failed in parsing response: {}", response.getBody());
            }
        }


        return reportsList;
    }

    @Override
    public String downloadReport(Map<String, String> headersMap, int fileId)
            throws HttpException, HttpRetryableException {

        String reportDownloadUrl = buildReportDownloadUrl(fileId);
        HttpCustomResponse response = httpService.get(reportDownloadUrl, headersMap);

        String data = null;
        if (response.getStatusCode().equals((long) HttpStatus.OK.value())) {
            log.info("Successfully requested List of available Reports with status NEW");
            try {
                data = response.getBody();
            } catch (Exception e){
                log.error("Failed in parsing response: {}", response.getBody());
            }
        }
        return data;
    }


    private String buildUrlPrefix() {
        String urlPrefix = indexExchangeConfig.baseUrl + "/"
                + indexExchangeConfig.version + "/"
                + indexExchangeConfig.reportEndPoint;
        log.info("URL base from configs: {}", urlPrefix);
        return urlPrefix;
    }

    private String buildListReportsUrl(int reportId){

        String baseUrl = buildUrlPrefix();
        String url = baseUrl + "/" + indexExchangeConfig.reportListEndPoint
                + "?accountIDs=" + indexExchangeConfig.accountId + "&status=new&reportIDs="
                + reportId;

        log.info(url);
        return url;
    }

    private String buildReportDownloadUrl(int fileId){

        String baseUrl = buildUrlPrefix();
        String url = baseUrl + "/" + indexExchangeConfig.reportDownloadEndPoint
                + "/" + fileId;

        log.info(url);
        return url;
    }
}