package com.target.kelsaapi.common.vo.facebook;

import com.facebook.ads.sdk.APINodeList;
import com.facebook.ads.sdk.AdReportRun;
import com.facebook.ads.sdk.AdsInsights;
import com.google.api.client.util.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Parses the APIResponse objects into a list of JsonElements from Facebook's AdsInsights Batch API results
 */
@Data
@Slf4j
public class FacebookAdsInsightsResponse {

    protected List<String> adAccountIds = Lists.newArrayList();

    protected List<String> campaignIds = Lists.newArrayList();

    protected List<String> adAccountIdsWithImpressions = Lists.newArrayList();

    protected List<AdReportRun> adReportRuns = Lists.newArrayList();
    /**
     * List of parsed APIResponses into Strings
     */
    protected List<String> adsInsightsReportResults = Lists.newArrayList();

    public void setAdsInsightsReportResults(APINodeList<AdsInsights> downloadedInsights) {
        for (AdsInsights insight : downloadedInsights) {
            adsInsightsReportResults.add(insight.getRawResponse());
        }
    }

}
