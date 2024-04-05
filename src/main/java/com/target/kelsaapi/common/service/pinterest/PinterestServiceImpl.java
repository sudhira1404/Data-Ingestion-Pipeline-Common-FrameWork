package com.target.kelsaapi.common.service.pinterest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.gson.Gson;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.pinterest.PinterestAdAccountsResponse;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequest;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequestResponseData;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportUrlResponseData;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.core5.http.ContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
@Service
public class PinterestServiceImpl implements PinterestService {

    private final HttpService httpService;

    private final PipelineConfig.Pinterest pinterestConfig;

    private final LocalFileWriterService localFileWriterService;

    @Autowired
    public PinterestServiceImpl(HttpService httpService, PipelineConfig config, LocalFileWriterService localFileWriterService) {
        this.httpService = httpService;
        this.pinterestConfig = config.getApiconfig().getSource().getPinterest();
        this.localFileWriterService = localFileWriterService;
    }

    @Override
    public List<Path> getPinterestData(PinterestReportRequest request, Oauth oauth, String pipelineRunId, String reportType) throws IOException {
        String tempFileRoot = CommonUtils.generateTempFileRootPath() + pipelineRunId;
        String adAccountFile = tempFileRoot + "-ad_accounts" + "-report-" + request.getStart_date() + ApplicationConstants.FileExtensions.CSV.getName();

        List<Path> localFiles = new ArrayList<>();
        //Initialize header
        Map<String,String> headersMap = setHeaderMap(oauth);
        //Get Ad Accounts
        Map<String, String> adAccounts = getAdAccounts(headersMap);
        List<String> printableAdAccounts = Lists.newArrayList();
        printableAdAccounts.add("ad_account_id,ad_account_name");


        for (Map.Entry<String,String> adAccount : adAccounts.entrySet()) {
            String accountId = adAccount.getKey();
            String accountName = adAccount.getValue();
            printableAdAccounts.add(accountId + "," + accountName);

            //Wait for report to be downloadable
            log.info("Getting report download URL for account: {}", accountName);
            URL reportDownloadLink;
            try {
                reportDownloadLink = new URL(getReportDownloadUrl(request, oauth, accountId, headersMap));

                log.info("Downloading report for account: {}", accountName);
                localFiles.addAll(localFileWriterService.writeLocalFile(reportDownloadLink,
                        tempFileRoot + "-" + accountId + "-report-" + request.getStart_date(),
                        ApplicationConstants.FileExtensions.CSV.getName(),
                        false, null, null, false));
            } catch (HttpException | JsonProcessingException | RuntimeException | HttpRetryableException e) {
                log.error(e.getMessage(), e.getCause());
            } catch (InterruptedException ie) {
                log.error(ie.getMessage(),ie.getCause());
                break;
            }
        }

        try {
            log.info("Writing Ad Account IDs and Names to file: {} ", adAccountFile);
            localFileWriterService.writeLocalFile(printableAdAccounts, adAccountFile,false, false);
            localFiles.add(Paths.get(adAccountFile));
            log.info("Successfully wrote Ad Account IDs and Names to file: {}", adAccountFile);
        } catch (NullPointerException | UnsupportedOperationException | ClassCastException | IllegalArgumentException e) {
            log.error("Failed writing Ad Account IDs and Names to file: {}", adAccountFile);
            log.error(e.getMessage(), e.getCause());
        }


        if (!localFiles.isEmpty()) {
            return localFiles;
        } else {
            throw new RuntimeException("There were no results for any Ad Accounts!");
        }
    }

    private String getReportDownloadUrl(PinterestReportRequest request, Oauth oauth, String accountId, Map<String, String> headersMap) throws HttpException, JsonProcessingException, HttpRetryableException, InterruptedException {

        //Fetch token
        String token = requestToken(headersMap, request, accountId);
        //Build tokenized URL
        String tokenizedUrl = buildTokenizedUrl(token, accountId);
        String returnUrl = null;
        ObjectMapper mapper = new ObjectMapper();
        int delay = 0;
        while (true) {
            HttpCustomResponse serviceResponse = httpService.get(tokenizedUrl, headersMap);
            PinterestReportUrlResponseData response = mapper.readValue(serviceResponse.getBody(), PinterestReportUrlResponseData.class);
            if (checkReportDownloadStatus(response)) {
                log.info("Requested report is finished, ready to download.");

                log.debug("Downloadable URL: {}", response.getUrl());
                returnUrl = response.getUrl();
                break;
            }
            log.info("Sleeping 30 seconds for report, current status: {}", response.getReport_status());
            try {
                Thread.sleep(1000*30);
            } catch (InterruptedException e) {
                throw new InterruptedException(e.getMessage());
            }
            delay = delay + (1000*30);
            if (delay > (1000*60*30)) {
                throw new RuntimeException("Timed out waiting for report to finish!");
            }
        }
        return returnUrl;
    }

    private Map<String, String> setHeaderMap(Oauth oauth) {
        Map<String,String> headersMap = new LinkedHashMap<>();
        headersMap.put("Authorization","Bearer " + oauth.getOAuthToken());
        headersMap.put("Accept", MediaType.APPLICATION_JSON_VALUE);
        headersMap.put("Content-Type", ContentType.APPLICATION_JSON.toString());
        return headersMap;
    }

    private String requestToken(Map<String, String> headersMap, PinterestReportRequest request, String accountId) throws HttpException, JsonProcessingException {
        String url = buildBaseUrl(accountId);
        Gson gson = new Gson();
        HttpCustomResponse response = httpService.post(url, headersMap, gson.toJson(request));
        ObjectMapper mapper = new ObjectMapper();
        PinterestReportRequestResponseData pinterestData = mapper.readValue(response.getBody(), PinterestReportRequestResponseData.class);
        log.debug("Token for report request: {}", pinterestData.getToken());
        return pinterestData.getToken();
    }

    private Boolean checkReportDownloadStatus(PinterestReportUrlResponseData response) {
        String reportStatus = response.getReport_status();
        return reportStatus.equals("FINISHED");
    }

    private String buildTokenizedUrl(String token, String accountId) {
        String tokenizedUrl = buildBaseUrl(accountId) + "/?token=" + token;
        log.debug("Tokenized URL: {}", tokenizedUrl);
        return tokenizedUrl;
    }

    private String buildBaseUrl(String accountId) {
        String urlPrefix = buildUrlPrefix();
        log.info("Account ID: {}", accountId);
        String startingUrl = urlPrefix + "/" + accountId + "/" + pinterestConfig.endpointSuffix;
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
