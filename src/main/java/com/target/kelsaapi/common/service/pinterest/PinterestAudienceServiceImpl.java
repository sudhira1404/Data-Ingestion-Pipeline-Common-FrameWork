package com.target.kelsaapi.common.service.pinterest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.pinterest.PinterestAdAccountsResponse;
import com.target.kelsaapi.common.vo.pinterest.PinterestAudienceResponse;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequest;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
@Service
public class PinterestAudienceServiceImpl implements PinterestAudienceService {

    private final HttpService httpService;

    private final PipelineConfig.Pinterest pinterestConfig;

    private final LocalFileWriterService localFileWriterService;

    @Autowired
    public PinterestAudienceServiceImpl(HttpService httpService, PipelineConfig config, LocalFileWriterService localFileWriterService) {
        this.httpService = httpService;
        this.pinterestConfig = config.getApiconfig().getSource().getPinterest();
        this.localFileWriterService = localFileWriterService;
    }

    @Override
    public List<String> getPinterestAudienceData(PinterestReportRequest request, Oauth oauth, String pipelineRunId, String reportType) throws IOException {
        String tempFileRoot = CommonUtils.generateTempFileRootPath() + pipelineRunId;
        String adAccountFile = tempFileRoot + "-audience" + "-report-" + request.getStart_date() + ApplicationConstants.FileExtensions.JSON.getName();

        //Initialize header
        Map<String,String> headersMap = setHeaderMap(oauth);
        //Get Ad Accounts
        Map<String, String> adAccounts = getAdAccounts(headersMap);
        List<String> printableAdAccounts = Lists.newArrayList();
        printableAdAccounts.add("ad_account_id,ad_account_name");

        List<String> finalList = new ArrayList<>();
        for (Map.Entry<String,String> adAccount : adAccounts.entrySet()) {
            String accountId = adAccount.getKey();
            String accountName = adAccount.getValue();
            printableAdAccounts.add(accountId + "," + accountName);

            //Wait for report to be downloadable
            log.info("Getting Audience data for account: {}", accountName);
            try {

                List<String> results  = getAudienceData(request, oauth, accountId, headersMap);

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
                break;
            }
        }

        if (!finalList.isEmpty()) {
            log.info("Total coount of audience result set is " + finalList.size());
            return finalList;
        } else {
            throw new RuntimeException("There were no results for any Ad Accounts!");
        }
    }

    private List<String> getAudienceData(PinterestReportRequest request, Oauth oauth, String accountId, Map<String, String> headersMap) throws HttpException, JsonProcessingException, HttpRetryableException, InterruptedException {

        String bookmark;
        Integer pageSize = 250;

        List<String> resultList = new ArrayList<>();
        {
            String url = buildBaseUrl(accountId, pageSize);
            HttpCustomResponse serviceResponse = httpService.get(url, headersMap);
            ObjectMapper mapper = new ObjectMapper();
            PinterestAudienceResponse pinterestData = mapper.readValue(serviceResponse.getBody(), PinterestAudienceResponse.class);
            bookmark = pinterestData.getBookmark();
            log.debug("Bookmark is : " + pinterestData.getBookmark());
            resultList.add(serviceResponse.getBody());
        }

            while (bookmark != null) {
                String urlWithBookmark = buildBaseUrlWithBookmark(accountId, pageSize, bookmark);
                HttpCustomResponse serviceResponse = httpService.get(urlWithBookmark, headersMap);
                ObjectMapper mapper = new ObjectMapper();
                PinterestAudienceResponse pinterestData = mapper.readValue(serviceResponse.getBody(), PinterestAudienceResponse.class);
                bookmark = pinterestData.getBookmark();
                log.debug("Bookmark is : " + pinterestData.getBookmark());
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


    private String buildBaseUrl(String accountId, Integer pageSize) {
        String urlPrefix = buildUrlPrefix();
        log.info("Account ID: {}", accountId);
//        String startingUrl = urlPrefix + "/" + accountId + "/" + pinterestConfig.endpointSuffix;
        String startingUrl = urlPrefix + "/" + accountId + "/" + "audiences" + "/?page_size=" + pageSize;
        log.info("Final URL to decorate: {}",startingUrl);
        return startingUrl;
    }

    private String buildBaseUrlWithBookmark(String accountId, Integer pageSize, String bookmark) {
        String urlPrefix = buildUrlPrefix();
        log.info("Account ID: {}", accountId);
//        String startingUrl = urlPrefix + "/" + accountId + "/" + pinterestConfig.endpointSuffix;
        String startingUrl = urlPrefix + "/" + accountId + "/" + "audiences" + "/?page_size=" + pageSize + "&bookmark=" + bookmark;
        log.info("Final URL to decorate: {}",startingUrl);
        return startingUrl;
    }

    private String buildUrlPrefix() {
        String urlPrefix = pinterestConfig.baseUrl + "/" + pinterestConfig.version + "/" + pinterestConfig.baseEndpoint;
        log.info("URL base from configs: {}", urlPrefix);
        return urlPrefix;
    }


    private Map<String, String> getAdAccounts(Map<String, String> headersMap) throws HttpException, HttpRetryableException, JsonProcessingException, RuntimeException {
        String url = buildUrlPrefix();
        HttpCustomResponse response = httpService.get(url, headersMap);
        ObjectMapper mapper = new ObjectMapper();
        PinterestAdAccountsResponse adAccountsResponse = mapper.readValue(response.getBody(), PinterestAdAccountsResponse.class);
        Map<String, String> adAccounts = new LinkedHashMap<>();
        int itemObjects = 1;
        try {
            List<PinterestAdAccountsResponse.Item> items = adAccountsResponse.getItems();
            itemObjects = items.size();
            log.info("Retrieved a total of {} Ad Accounts", itemObjects);
            for (PinterestAdAccountsResponse.Item item : items) {
                adAccounts.put(item.getId(), item.getName());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
        }

        if (!adAccounts.isEmpty() && Objects.equals(adAccounts.size(),itemObjects)) {
            log.info("Successfully deserialized all Ad Accounts");
            return adAccounts;
        } else {
            throw new RuntimeException("Failed to deserialize all Ad Accounts from the response! Received "+
                    itemObjects+" Ad Account responses, but only deserialized "+ adAccounts.size()+ "IDs");
        }
    }
}
