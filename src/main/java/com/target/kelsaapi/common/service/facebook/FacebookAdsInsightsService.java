package com.target.kelsaapi.common.service.facebook;

import com.facebook.ads.sdk.*;
import com.target.kelsaapi.common.exceptions.FacebookException;
import com.target.kelsaapi.common.vo.facebook.FacebookAdsInsightsRequest;
import com.target.kelsaapi.common.vo.facebook.FacebookAdsInsightsResponse;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Service layer for interacting with the Facebook Java Ads SDK for Ads Insights
 */
@Service("facebookAdsInsightsService")
@Slf4j
public class FacebookAdsInsightsService implements FacebookService {

    private final int SLEEP_INTERVAL_MILLISECONDS;

    private final int MAX_SLEEP_INTERVALS;

    protected final PipelineConfig config;

    protected APIContext context;

    @Autowired
    FacebookAdsInsightsService(PipelineConfig config) {
        this.config = config;
        this.SLEEP_INTERVAL_MILLISECONDS = config.apiconfig.source.facebook.sleepIntervalMs;
        this.MAX_SLEEP_INTERVALS = config.apiconfig.source.facebook.maxSleepIntervals;
        setApiContext();
    }

    protected enum FacebookServiceMethodNames {
        GET_OWNED_AD_ACCOUNT_IDS,
        GET_AD_ACCOUNTS_WITH_IMPRESSIONS,
        REQUEST_AD_REPORT_RUNS,
        FETCH_REPORT_RESULTS
    }

    /**
     * Initializes the APIContext for Facebook
     */
    protected void setApiContext() {
        // get initial Facebook configs
        final String id = config.apiconfig.source.facebook.appId;
        final String token = config.apiconfig.source.facebook.context.accessToken;
        final String secret = config.apiconfig.source.facebook.context.appSecret;
        final Boolean debug = config.apiconfig.source.facebook.context.debug;


        // Initialize Facebook Service and Context
        this.context = new APIContext(token, secret, id);
        context.enableDebug(debug);
        if (debug) {
            context.setLogger(new PrintStream(new FacebookLoggingStream(log, FacebookLoggingStream.LogLevel.DEBUG)));
        } else {
            context.setLogger(new PrintStream(new FacebookLoggingStream(log, FacebookLoggingStream.LogLevel.INFO)));
        }
        log.debug("App ID from context: " + context.getAppID());
    }

    /**
     * The main sequencer of all steps to retrieve {@link AdsInsights} for all {@link AdAccount}s having Impressions
     * for the requested {@link FacebookAdsInsightsRequest#timeRangeFormatted} date range.
     *
     * @param facebookAdsInsightsRequest The {@link FacebookAdsInsightsRequest} object.
     * @return A {@link FacebookAdsInsightsResponse} object.
     * @throws RuntimeException When any step fails to produce expected results within the configured max retries.
     */
    @Override
    public FacebookAdsInsightsResponse getApiData(FacebookAdsInsightsRequest facebookAdsInsightsRequest)
            throws FacebookException {
        //Initialize Response object
        FacebookAdsInsightsResponse facebookAdsInsightsResponse = new FacebookAdsInsightsResponse();

        //Retrieve the list of Owned Ad Accounts
        trySleepRetryWrapper(FacebookServiceMethodNames.GET_OWNED_AD_ACCOUNT_IDS, facebookAdsInsightsRequest, facebookAdsInsightsResponse);
        if (facebookAdsInsightsResponse.getAdAccountIds().size() == 0) {
            throw new FacebookException("Unable to request Ad Accounts owned by the Business Account.");
        }

        //Retrieve Ad Accounts with Impressions
        trySleepRetryWrapper(FacebookServiceMethodNames.GET_AD_ACCOUNTS_WITH_IMPRESSIONS, facebookAdsInsightsRequest, facebookAdsInsightsResponse);
        if (facebookAdsInsightsResponse.getAdAccountIdsWithImpressions().size() == 0) {
            throw new FacebookException("Unable to request Ad Accounts with Impressions");
        }

        //Generate Insight Report Run Requests
        trySleepRetryWrapper(FacebookServiceMethodNames.REQUEST_AD_REPORT_RUNS, facebookAdsInsightsRequest, facebookAdsInsightsResponse);
        if (facebookAdsInsightsResponse.getAdReportRuns().size() == 0) {
            throw new FacebookException("Unable to request Ad Report Runs for all Ad Accounts with Impressions");
        }

        //Wait until all Report Run IDs have finished or failed.
        // This doesn't use the trySleepRetryWrapper method. It instead handles the sleeping/retry logic internally
        // to the method, since it first sleeps, then tries before retrying again.
        if (!getReportRunStatuses(facebookAdsInsightsResponse)) {
            throw new FacebookException("Failure detected while waiting for all Ad Report Run statuses to be completed");
        }

        //Fetch Report Results
        trySleepRetryWrapper(FacebookServiceMethodNames.FETCH_REPORT_RESULTS, facebookAdsInsightsRequest, facebookAdsInsightsResponse);
        if (facebookAdsInsightsResponse.getAdsInsightsReportResults().size() == 0) {
            throw new FacebookException("Unable to download insights from API after " + MAX_SLEEP_INTERVALS + " attempts");
        }
        return facebookAdsInsightsResponse;
    }

    /**
     * Uses the {@link Business.APIRequestGetOwnedAdAccounts} method to retrieve the list of {@link AdAccount} IDs
     * which are owned by the configured business ID. The IDs are then added to the {@link FacebookAdsInsightsResponse#adAccountIds}
     * list for downstream methods to utilize. This should be the first method called prior to calling
     * {@link #getAdAccountsWithImpressions(FacebookAdsInsightsRequest, FacebookAdsInsightsResponse)}
     *
     * @return True if the {@link FacebookAdsInsightsResponse#adAccountIds} is populated with at least one value.
     * False if the list is empty.
     */
    protected Boolean getOwnedAdAccountIds(FacebookAdsInsightsResponse facebookAdsInsightsResponse) {
        List<String> ownedAdAccountIds = facebookAdsInsightsResponse.getAdAccountIds();
        try {
            // Initialize API Request object for Business Owned Ad Accounts
            Business.APIRequestGetOwnedAdAccounts ownedAdAccounts = new Business
                    .APIRequestGetOwnedAdAccounts(context.getAppID(), context);

            // Execute API Request and store the resulting API Node List of Ad Account objects
            APINodeList<AdAccount> result = ownedAdAccounts.execute().withAutoPaginationIterator(true);

            // Loop through API Node List, for each Ad Account, gets the ID, and adds to the final ownedAdAccountIds List
            for (AdAccount adAccount : result) {
                String id = adAccount.getFieldAccountId();
                ownedAdAccountIds.add(id);
            }

        } catch (APIException e) {
            log.error(e.getMessage(), e.getCause());
            return false;
        }
        return true;
    }

    /**
     * Loops through the list of {@link FacebookAdsInsightsResponse#adAccountIds} to determine which of those {@link AdAccount} IDs
     * has an Impression count > 0 for the given {@link FacebookAdsInsightsRequest#timeRangeFormatted}. Those that do are
     * then added to the {@link FacebookAdsInsightsResponse#adAccountIdsWithImpressions} list. Ideally this method should be
     * called after calling the {@link #getOwnedAdAccountIds(FacebookAdsInsightsResponse)} method to initialize the
     * {@link FacebookAdsInsightsResponse} object properly.
     *
     * @param facebookAdsInsightsRequest The {@link FacebookAdsInsightsRequest} object
     * @param facebookAdsInsightsResponse The {@link FacebookAdsInsightsResponse} object
     * @return True if there is at least 1 {@link AdAccount} in the {@link FacebookAdsInsightsResponse#adAccountIdsWithImpressions}
     * list. False if the list is empty.
     */
    protected Boolean getAdAccountsWithImpressions(FacebookAdsInsightsRequest facebookAdsInsightsRequest,
                                                      FacebookAdsInsightsResponse facebookAdsInsightsResponse) {

        List<String> campaignIds = facebookAdsInsightsResponse.getCampaignIds();
        // Campaign IDs should be empty, will attempt to empty contents of list if prior values exist
        if (Boolean.FALSE.equals(isListStringEmpty(campaignIds, "campaignIds", true))) {
            return false;
        }

        List<String> adAccountIdsWithImpressions = facebookAdsInsightsResponse.getAdAccountIdsWithImpressions();
        // adAccountIdsWithImpressions should be empty, will attempt to empty contents of list if prior values exist
        if (Boolean.FALSE.equals(isListStringEmpty(adAccountIdsWithImpressions,
                "adAccountIdsWithImpressions", true))) {
            return false;
        }

        List<String> adAccountIds = facebookAdsInsightsResponse.getAdAccountIds();
        // adAccountIds must not be empty
        if (Boolean.TRUE.equals(isListStringEmpty(adAccountIds, "adAccountIds", false))) {
            return false;
        }

        int sizeOf = adAccountIds.size();
        log.info("Begin retrieving the list of campaign IDs for {} Ad Account IDs which have impression counts " +
                "greater than 0 for the given time frame.", sizeOf);

        for (String adAccount : adAccountIds) {

            String adAccountId = "act_" + adAccount;
            log.info("Fetching campaigns with activity during the requested dates for Ad Account ID : {}", adAccountId);
            AdAccount.APIRequestGetInsights activeCampaigns = new AdAccount.APIRequestGetInsights(adAccountId, context);

            APINodeList<AdsInsights> adsInsights;
            try {
                activeCampaigns.setParam("level", facebookAdsInsightsRequest.getLevel())
                        .setParam("time_range", facebookAdsInsightsRequest.getTimeRangeFormatted())
                        .setParam("time_increment", facebookAdsInsightsRequest.getTimeIncrement())
                        .setParam("filtering", "[" + facebookAdsInsightsRequest.getCampaignFilterJson() + "]")
                        .setParam("limit", 300);
                activeCampaigns.setFields("campaign_id");
                adsInsights = activeCampaigns.execute().withAutoPaginationIterator(true);

                List<String> innerCount = new ArrayList<>();
                for (AdsInsights adsInsight : adsInsights) {
                    String campaignId = adsInsight.getFieldCampaignId();
                    if (campaignId != null) {
                        campaignIds.add(campaignId);
                        log.debug("Campaign ID {} has activity!", campaignId);
                        innerCount.add(campaignId);
                    }
                }
                int innerCountSize = innerCount.size();
                if (innerCountSize > 0) {
                    log.info("Total campaigns fetched for account ID {} : {}", adAccountId, innerCountSize);
                    adAccountIdsWithImpressions.add(adAccountId);
                } else {
                    log.warn("No campaigns were active for account ID {} on the date range specified.", adAccountId);
                }

            } catch (APIException e) {
                log.error(e.getMessage());
                return false;
            }
        }
        int finalCampaignSize = campaignIds.size();
        int finalAdAccountSize = adAccountIdsWithImpressions.size();
        if (finalCampaignSize > 0) {
            log.info("Total campaigns with impressions : " + finalCampaignSize);
            log.info("Total Ad Accounts with active campaigns : " + finalAdAccountSize);
            log.info("List of Ad Accounts with campaigns : " + adAccountIdsWithImpressions);
            return true;
        } else {
            String errorMessage = "No campaigns were found active for any Ad Accounts in this date range!";
            log.error(errorMessage);
            return false;
        }
    }

    /**
     * Uses {@link AdAccount.APIRequestGetInsightsAsync} to generate asynchronous {@link AdReportRun} objects which
     * will eventually produce downloadable {@link AdsInsights} data for each {@link AdAccount} which is stored in the
     * {@link FacebookAdsInsightsResponse#adAccountIdsWithImpressions} list.
     *
     * Ideally the {@link #getAdAccountsWithImpressions(FacebookAdsInsightsRequest, FacebookAdsInsightsResponse)} method
     * was called prior to calling this method so that the same {@link FacebookAdsInsightsResponse} object was properly
     * initialized.
     *
     * @param facebookAdsInsightsRequest An initialized {@link FacebookAdsInsightsRequest} object,
     *                                   with all fields already populated by their setter methods.
     * @param facebookAdsInsightsResponse An initialized {@link FacebookAdsInsightsResponse} object.
     *                                    The field {@link FacebookAdsInsightsResponse#adReportRuns} should be empty,
     *                                    and the field {@link FacebookAdsInsightsResponse#adAccountIdsWithImpressions}
     *                                    should be populated.
     * @return True if all Ad Accounts with Impressions had Ad Report Runs successfully requested; False if not.
     *
     * @see  <a href="https://developers.facebook.com/docs/marketing-api/insights/best-practices/">
     *     This approach follows Facebook's recommendations as described here:
     *     developers.facebook.com/docs/marketing-api/insights/best-practices</a>
     */
    protected Boolean requestAdReportRuns(FacebookAdsInsightsRequest facebookAdsInsightsRequest,
                                        FacebookAdsInsightsResponse facebookAdsInsightsResponse) {

        List<AdReportRun> adReportRuns = facebookAdsInsightsResponse.getAdReportRuns();
        //adReportRuns needs to be empty
        if (!isListAdReportRunEmpty(adReportRuns)) return false;

        //adAccountsWithImpressions needs to already be populated
        List<String> adAccountsWithImpressions = facebookAdsInsightsResponse.getAdAccountIdsWithImpressions();
        if (isListStringEmpty(adAccountsWithImpressions, "adAccountIdsWithImpressions", false)) {
            return false;
        }

        log.info("Looping through {} Ad Accounts with Impressions to generate Ad Report Run requests",
                adAccountsWithImpressions.size());

        for (String adAccount : adAccountsWithImpressions) {
            AdAccount.APIRequestGetInsightsAsync async = new AdAccount.APIRequestGetInsightsAsync(adAccount, context);
            async.setLevel(AdsInsights.EnumLevel.VALUE_CAMPAIGN.toString())
                    .setTimeRange(facebookAdsInsightsRequest.timeRangeFormatted)
                    .setTimeIncrement("1")
                    .setFiltering("[" + facebookAdsInsightsRequest.campaignFilterJson + "]")
                    .setActionAttributionWindows(facebookAdsInsightsRequest.actionAttributionWindows)
                    .setActionBreakdowns(facebookAdsInsightsRequest.actionBreakdowns)
                    .setBreakdowns(facebookAdsInsightsRequest.breakdowns)
                    .setActionReportTime(facebookAdsInsightsRequest.actionReportTime);

            AdReportRun adReportRun;
            try {
                adReportRun = async.requestFields(facebookAdsInsightsRequest.requestFields).execute();
            } catch (APIException e) {
                log.error(e.getMessage(), e.getCause());
                return false;
            }

            if (adReportRun == null) {
                log.error("Unable to request Ad Report Run for Ad Account {}", adAccount);
                return false;
            } else {
                log.debug("Successfully requested Ad Report Run ID {} for Ad Account {}", adReportRun.getFieldId(),
                        adAccount);
                adReportRuns.add(adReportRun);
            }
        }
        log.info("Total number of Ad Report Runs: " + adReportRuns.size());
        return true;
    }

    /**
     * Uses the current instance of {@link FacebookAdsInsightsResponse#adReportRuns} to {@link AdReportRun#fetch()} the
     * updated status for each {@link AdReportRun} in the list. This will keep sleeping and retrying until all report
     * runs are in completed state; one or more of the report runs are failed; or it reaches the maximun number of
     * retries configured.
     *
     * @param facebookAdsInsightsResponse The {@link FacebookAdsInsightsResponse} object.
     * @return True if the status of all {@link AdReportRun} objects are completed. False if any have failed, or
     * if it has exhausted all possible retries.
     */
    protected Boolean getReportRunStatuses(FacebookAdsInsightsResponse facebookAdsInsightsResponse) {
        int totalCompleted = 0;
        int totalFailed = 0;

        for (int i = 0; i <= MAX_SLEEP_INTERVALS; i++) {
            log.info("Sleeping for {} milliseconds before checking status of all Ad Reports", SLEEP_INTERVAL_MILLISECONDS);
            try {
                Thread.sleep(SLEEP_INTERVAL_MILLISECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e.getCause());
                return false;
            }
            log.info("Woke up, now checking...");
            for (AdReportRun report : facebookAdsInsightsResponse.getAdReportRuns()) {
                String currentStatus;
                try {
                    currentStatus = report.fetch().getFieldAsyncStatus();
                } catch (APIException e) {
                    log.error(e.getMessage(), e.getCause());
                    currentStatus="Job Failed";
                }
                log.debug("Current status {} of Ad Report {}", currentStatus, report.getId());
                totalCompleted = totalCompleted + completedCheck(currentStatus);
                totalFailed = totalFailed + failedCheck(currentStatus);
            }
            if (facebookAdsInsightsResponse.getAdReportRuns().size() == totalCompleted) {
                log.info("All reports are completed and ready for download");
                return true;
            } else if (totalFailed > 0) {
                log.error("{} Failed reports detected", totalFailed);
                return false;
            }
            //Reset to 0 for next loop check
            totalCompleted = 0;
            totalFailed = 0;
        }
        return false;
    }

    /**
     * Helper method for {@link #getReportRunStatuses(FacebookAdsInsightsResponse)} used to determine if the status
     * of the {@link AdReportRun} is completed.
     *
     * @param currentStatus The status of the {@link AdReportRun}
     * @return 0 if it has not completed; 1 if it has completed.
     */
    private static int completedCheck(String currentStatus) {
        if (currentStatus.equals("Job Completed")) return 1;
        return 0;
    }

    /**
     * Helper method for {@link #getReportRunStatuses(FacebookAdsInsightsResponse)} used to determine if the
     * status of the {@link AdReportRun} is failed.
     *
     * @param currentStatus The status of the {@link AdReportRun}.
     * @return 0 if it has not failed; 1 if it has failed.
     */
    private static int failedCheck(String currentStatus) {
        if (currentStatus.equals("Job Failed") || currentStatus.equals("Job Skipped")) return 1;
        return 0;
    }

    /**
     * Retrieves the results of Ad Report Runs using {@link AdAccount.APIRequestGetInsights}.
     * Relies on the {@link FacebookAdsInsightsResponse#getAdReportRuns()} method to produce
     * a {@link List} of {@link AdReportRun} to loop over to retrieve results from. This assumes that each
     * {@link AdReportRun} is in a completed state. Use {@link #getReportRunStatuses(FacebookAdsInsightsResponse)}
     * prior to calling this method in order to wait for all the report runs to be completed.
     *
     * Once downloaded, all results are then added to the {@link FacebookAdsInsightsResponse#adsInsightsReportResults} field.
     *
     * @param facebookAdsInsightsResponse An instance of the {@link FacebookAdsInsightResponse} object.
     * @return True indicates all downloaded results were successfully added to the
     * {@link FacebookAdsInsightsResponse#adsInsightsReportResults} field. False indicates a failure.
     *
     */
    protected Boolean fetchReportResults(FacebookAdsInsightsResponse facebookAdsInsightsResponse) {

        APINodeList<AdsInsights> downloadedInsights = null;
        for (AdReportRun download : facebookAdsInsightsResponse.getAdReportRuns()) {
            String reportId = download.getId();
            AdAccount.APIRequestGetInsights downloadable = new AdAccount.APIRequestGetInsights(reportId, context);
            try {
                downloadedInsights = downloadable.execute().withAutoPaginationIterator(true);
            } catch (APIException e) {
                log.error(e.getMessage(), e.getCause());
                return false;
            }
        }
        if (downloadedInsights == null) {
            log.error("No insights were downloaded.");
            return false;
        } else {
            facebookAdsInsightsResponse.setAdsInsightsReportResults(downloadedInsights);
            log.info("Total Ads Insights results: " + facebookAdsInsightsResponse.getAdsInsightsReportResults().size());
            return true;
        }
    }

    /**
     * This convenience method is used to validate and/or initialize an empty list.
     *
     * @param list The {@link List} to validate is empty or not.
     * @param name The name to use for logging results.
     * @param emptyContentsIfFull Set to True if the list should be emptied if it has existing records in it.
     *                            Otherwise set to False. Setting to False in the situation where the list has records
     *                            on it will cause the result of the method to be False, but would preserve the contents
     *                            of the list.
     * @return True if the list is empty. False if the list is not empty.
     */
    private Boolean isListStringEmpty(List<String> list, String name, Boolean emptyContentsIfFull) {
        if (list.size() > 0 && Boolean.TRUE.equals(emptyContentsIfFull)) {
            log.warn("{} field was not empty. Will empty contents and retry.", name);
            for (String remove : list) {
                list.remove(remove);
            }
            if (list.size() == 0) {
                log.info("Successfully removed all prior {}s from the list.", name);
                return true;
            } else {
                log.error("Failed to remove {} {}s from the list.",
                        list.size(), name);
                return false;
            }
        } else if (list.size() > 0 && Boolean.FALSE.equals(emptyContentsIfFull)) {
            log.error("{} field is not empty.", name);
            return false;
        } else if (list.size() == 0) {
            log.info("{} field is empty.", name);
            return true;
        }
        return false;
    }

    /**
     * This convenience method is used to validate and/or initialize an empty list of {@link AdReportRun}.
     *
     * @param adReportRuns The {@link List} of {@link AdReportRun} objects to validate is empty or not.
     * @return True if the list is empty. False if it is not empty.
     */
    private Boolean isListAdReportRunEmpty(List<AdReportRun> adReportRuns) {
        if (adReportRuns.size() > 0) {
            log.warn("AdReportRuns field on FacebookAdsInsightsResponse object was not empty. Will empty contents and retry.");
            for (AdReportRun remove : adReportRuns) {
                adReportRuns.remove(remove);
            }
            if (adReportRuns.size() == 0) {
                log.info("Successfully removed all prior AdReportRuns from the FacebookAdsInsightsResponse object");
                return true;
            } else {
                log.error("Failed to remove {} AdReportRun objects from the FacebookAdsInsightsResponse object",
                        adReportRuns.size());
                return false;
            }
        } else {
            log.info("AdReportsRuns field on FacebookAdsInsightsResponse object is empty!");
            return true;
        }
    }

    /**
     * Convenience method to wrap around one of the public methods in this class and manage the try-sleep-retry
     * looping intervals.
     *
     * @param methodName The {@link FacebookServiceMethodNames} to try-sleep-retry
     * @param facebookAdsInsightsRequest The {@link FacebookAdsInsightsRequest} object
     * @param facebookAdsInsightsResponse The {@link FacebookAdsInsightsResponse} object
     */
    private void trySleepRetryWrapper(FacebookServiceMethodNames methodName, FacebookAdsInsightsRequest facebookAdsInsightsRequest,
                                      FacebookAdsInsightsResponse facebookAdsInsightsResponse) {

        for (int i=1; i<=MAX_SLEEP_INTERVALS; i++) {

            boolean runThis = false;
            String logString = "Undefined";
            switch (methodName) {
                case GET_OWNED_AD_ACCOUNT_IDS:
                    runThis = getOwnedAdAccountIds(facebookAdsInsightsResponse);
                    logString = "Ad Accounts owned by the Business Account";
                    break;
                case GET_AD_ACCOUNTS_WITH_IMPRESSIONS:
                    runThis = getAdAccountsWithImpressions(facebookAdsInsightsRequest, facebookAdsInsightsResponse);
                    logString = "Ad Account IDs with Impressions";
                    break;
                case REQUEST_AD_REPORT_RUNS:
                    runThis = requestAdReportRuns(facebookAdsInsightsRequest, facebookAdsInsightsResponse);
                    logString = "AdReportRuns for all Ad Accounts with Impressions ";
                    break;
                case FETCH_REPORT_RESULTS:
                    runThis = fetchReportResults(facebookAdsInsightsResponse);
                    logString = "fetchReportResults";
                    break;
                default:
                    break;
            }
            log.info("Attempt #{} to request {}", i, logString);
            if (runThis) {
                log.info("Successfully requested {}", logString);
                break;
            }
            log.info("Attempt unsuccessful. Will reattempt to request {} shortly.", logString);
            try {
                log.info("Sleep in between attempts for {} milliseconds", SLEEP_INTERVAL_MILLISECONDS);
                Thread.sleep(SLEEP_INTERVAL_MILLISECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e.getCause());
                break;
            }
        }
    }
}
