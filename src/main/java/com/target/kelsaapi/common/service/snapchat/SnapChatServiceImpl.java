package com.target.kelsaapi.common.service.snapchat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.HttpException;
import com.target.kelsaapi.common.exceptions.HttpRetryableException;
import com.target.kelsaapi.common.exceptions.PaginationException;
import com.target.kelsaapi.common.exceptions.SnapChatException;
import com.target.kelsaapi.common.service.rest.HttpService;
import com.target.kelsaapi.common.vo.HttpCustomResponse;
import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.snapchat.SnapChatAdAccounts;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Map;

import static com.target.kelsaapi.common.util.SnapChatUtils.*;

@Slf4j
@Service("snapChatService")
public class SnapChatServiceImpl implements SnapChatService {

    final private HttpService httpService;

    final private PipelineConfig.SnapChat snapChatConfig;

    final private String AD_ACCOUNTS = "adaccounts";

    @Autowired
    public SnapChatServiceImpl(HttpService httpService, PipelineConfig config) {
        this.httpService = httpService;
        this.snapChatConfig = config.apiconfig.source.snapchat;
    }

    @Override
    public ArrayList<String> getSnapChatCampaignDetails(Oauth oauth) throws SnapChatException {

        return getSnapChatDetails(oauth);

    }

    @Override
    public ArrayList<String> getSnapChatCampaignStats(Oauth oauth,String startDate, String endDate) throws SnapChatException {

        return getSnapChatDetails(oauth, ApplicationConstants.SnapChatReportTypes.STATS, startDate, endDate);

    }

    private ArrayList<String> getSnapChatDetails(Oauth oauth) throws SnapChatException {
        return getSnapChatDetails(oauth, ApplicationConstants.SnapChatReportTypes.CAMPAIGNS, null, null);
    }

    private ArrayList<String> getSnapChatDetails(Oauth oauth, ApplicationConstants.SnapChatReportTypes snapChatReportType, @Nullable String startDate, @Nullable String endDate) throws SnapChatException {

        ArrayList<SnapChatAdAccounts.AdAccount> adAccounts;
        try {
            adAccounts = getAdAccounts(oauth);
        } catch (HttpException | JsonProcessingException | PaginationException e) {
            throw new SnapChatException(e.getMessage(), e.getCause());
        }

        ArrayList<String> results = new ArrayList<>();
        String snapChatType = snapChatReportType.toString().toLowerCase();
        String attributePathInMessage = getAttributePathInMessage(snapChatReportType);

        adAccounts.forEach(account -> {
            String accountId = account.getId();
            String accountName = account.getName();
            String timeZone = account.getTimezone();


            String startingUrl = buildUrl(snapChatConfig.baseUrl, snapChatConfig.version, AD_ACCOUNTS, accountId, snapChatType);
            String finalUrl;

            Pair<String, String> formattedTimes;
            if (snapChatReportType.equals(ApplicationConstants.SnapChatReportTypes.STATS))  {
                formattedTimes = transformDatesToTimes(startDate, endDate, timeZone);
                finalUrl = buildDecoratedUrl(startingUrl, snapChatReportType, formattedTimes.getValue0(), formattedTimes.getValue1());
            } else {
                finalUrl = buildDecoratedUrl(startingUrl, snapChatReportType, startDate, endDate);
            }

            ArrayList<String> responseResults = null;
            try {
                responseResults = getResponse(oauth,finalUrl,attributePathInMessage);
            } catch (HttpException | JsonProcessingException | PaginationException e) {
                log.error(e.getMessage());
            }
            if (responseResults != null) {
                results.addAll(decorateHttpResponse(responseResults, accountId, accountName, snapChatReportType));
            } else {
                log.warn("No results from account ID: {}", accountId);
            }
        });
        return results;
    }

    private ArrayList<SnapChatAdAccounts.AdAccount> getAdAccounts(Oauth oauth) throws HttpException, JsonProcessingException, PaginationException {

        String url = buildUrl(snapChatConfig.baseUrl, snapChatConfig.version,"organizations", snapChatConfig.getAccountId(), AD_ACCOUNTS);
        log.debug("SnapChat Organization Ad Accounts url: {}", url);

        ArrayList<SnapChatAdAccounts.AdAccount> returnable = new ArrayList<>();
        ArrayList<String> response = getResponse(oauth, url,"paging/next_link");
        response.forEach(json -> {
            Gson gson = new GsonBuilder().create();
            SnapChatAdAccounts adAccounts = gson.fromJson(json, SnapChatAdAccounts.class);
            SnapChatAdAccounts.SnapChatAdAccount[] accountArray = adAccounts.getAdaccounts();
            for (SnapChatAdAccounts.SnapChatAdAccount account : accountArray) {
                returnable.add(account.getAdaccount());
            }
        });
        return returnable;
    }

    private ArrayList<String> getResponse(Oauth oauth, String url, String attributePathInMessage) throws HttpException, JsonProcessingException, PaginationException {

        ArrayList<String> result = new ArrayList<>();
        String deserializeResponse = callApi(oauth,url);
        if (deserializeResponse == null || deserializeResponse.isEmpty()) {
            log.warn("Last request made resulted in no data from API. Skipping and will not retry");
        } else {
            result.add(deserializeResponse);
            ObjectMapper mapper = new ObjectMapper();
            String nextLink = getNextLink(deserializeResponse, mapper, attributePathInMessage);
            int nextLinkCounter = 0;
            while (true) {
                if (nextLink != null && !nextLink.trim().isEmpty() && !nextLink.trim().equals("[]")) {
                    nextLinkCounter = nextLinkCounter + 1;
                    log.info("Next link counter: {}", nextLinkCounter);
                    deserializeResponse = callApi(oauth, nextLink);
                    result.add(deserializeResponse);
                    nextLink = getNextLink(deserializeResponse, mapper, attributePathInMessage);
                } else {
                    log.info("Total number of pages processed : {}", nextLinkCounter + 1);
                    break;
                }
                if (nextLinkCounter == 0) {
                    log.info("Pagination not working by traversing the path : {}", attributePathInMessage);
                    throw new PaginationException("Pagination not working by traversing the given input path");
                }
            }
        }
        return result;
    }

    private String callApi(Oauth oauth, String url) {

        log.debug("url Details: {}", url);
        Map<String, String> headers = setHttpHeaders(oauth);
        HttpCustomResponse response = null;
        try {
            response = httpService.get(url, headers);
        } catch (HttpException | HttpRetryableException e) {
            log.error(e.getMessage());
        }

        String httpResponse = null;
        try {
            assert response != null;
            httpResponse = getHttpResponse(response);
        } catch (AssertionError | HttpException e) {
            log.error(e.getMessage());
        }
        return httpResponse;
    }

}