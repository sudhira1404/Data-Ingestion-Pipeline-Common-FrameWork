package com.target.kelsaapi.common.vo.facebook;

import com.facebook.ads.sdk.AdsInsights;
import com.google.gson.Gson;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains all fields and pre-set parameters necessary to make requests to Facebook's Ad Insights API
 */
@Data
public class FacebookAdsInsightsRequest {

    public List<AdsInsights.EnumActionBreakdowns> actionBreakdowns = new ArrayList<>();

    public List<AdsInsights.EnumBreakdowns> breakdowns = new ArrayList<>();

    public List<AdsInsights.EnumActionAttributionWindows> actionAttributionWindows = new ArrayList<>();

    public List<String> requestFields = new ArrayList<>();

    public String timeRangeFormatted;

    public String campaignFilterJson;

    final public String level = AdsInsights.EnumLevel.VALUE_CAMPAIGN.toString();

    final public String actionReportTime = AdsInsights.EnumActionReportTime.VALUE_IMPRESSION.toString();

    final public String timeIncrement = "1";

    final public Boolean enableAutoPagination = true;

    /**
     * These fields are out of scope for pulling campaign-level insights
     */
    public enum AdAccountFieldExclusion {
        action_values,
        ad_bid_type,
        ad_bid_value,
        ad_click_actions,
        ad_delivery,
        ad_id,
        ad_impression_actions,
        ad_name,
        adset_bid_type,
        adset_bid_value,
        adset_budget_type,
        adset_budget_value,
        adset_delivery,
        adset_end,
        adset_id,
        adset_name,
        adset_start,
        age_targeting,
        auction_bid,
        auction_competitiveness,
        auction_max_competitor_bid,
        canvas_avg_view_percent,
        canvas_avg_view_time,
        catalog_segment_actions,
        catalog_segment_value,
        catalog_segment_value_mobile_purchase_roas,
        catalog_segment_value_omni_purchase_roas,
        catalog_segment_value_website_purchase_roas,
        conversion_rate_ranking,
        conversion_values,
        conversions,
        converted_product_quantity,
        converted_product_value,
        cost_per_15_sec_video_view,
        cost_per_2_sec_continuous_video_view,
        cost_per_ad_click,
        cost_per_conversion,
        cost_per_dda_countby_convs,
        cost_per_one_thousand_ad_impression,
        cost_per_store_visit_action,
        cost_per_unique_conversion,
        created_time,
        dda_countby_convs,
        engagement_rate_ranking,
        estimated_ad_recall_rate_lower_bound,
        estimated_ad_recall_rate_upper_bound,
        estimated_ad_recallers_lower_bound,
        estimated_ad_recallers_upper_bound,
        full_view_impressions,
        full_view_reach,
        gender_targeting,
        interactive_component_tap,
        labels,
        location,
        mobile_app_purchase_roas,
        place_page_name,
        purchase_roas,
        qualifying_question_qualify_answer_rate,
        quality_ranking,
        quality_score_ectr,
        quality_score_ecvr,
        quality_score_organic,
        social_spend,
        store_visit_actions,
        unique_conversions,
        unique_video_continuous_2_sec_watched_actions,
        unique_video_view_15_sec,
        updated_time,
        video_continuous_2_sec_watched_actions,
        video_play_curve_actions,
        video_play_retention_0_to_15s_actions,
        video_play_retention_20_to_60s_actions,
        video_play_retention_graph_actions,
        video_time_watched_actions,
        website_purchase_roas,
        wish_bid;

        /**
         * Used to verify whether a given string exists in this enum or not
         *
         * @param s The string to check whether it exists in the enum or not
         * @return True, the string exists in the enum; false it does not.
         */
        public static boolean contains(String s) {
            for (AdAccountFieldExclusion choice:values())
                if (choice.name().equals(s))
                    return true;
            return false;
        }
    }

    /**
     * These fields are in scope to pull for campaign-level insights
     */
    public enum AdAccountFields {
        account_currency,
        account_id,
        account_name,
        actions,
        buying_type,
        campaign_id,
        campaign_name,
        clicks,
        cost_per_action_type,
        cost_per_estimated_ad_recallers,
        cost_per_inline_link_click,
        cost_per_inline_post_engagement,
        cost_per_outbound_click,
        cost_per_thruplay,
        cost_per_unique_action_type,
        cost_per_unique_click,
        cost_per_unique_inline_link_click,
        cost_per_unique_outbound_click,
        cpc,
        cpm,
        cpp,
        ctr,
        date_start,
        date_stop,
        dda_results,
        estimated_ad_recall_rate,
        estimated_ad_recallers,
        frequency,
        impressions,
        inline_link_click_ctr,
        inline_link_clicks,
        inline_post_engagement,
        instant_experience_clicks_to_open,
        instant_experience_clicks_to_start,
        instant_experience_outbound_clicks,
        objective,
        outbound_clicks,
        outbound_clicks_ctr,
        reach,
        spend,
        unique_actions,
        unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        unique_inline_link_clicks,
        unique_link_clicks_ctr,
        unique_outbound_clicks,
        unique_outbound_clicks_ctr,
        video_15_sec_watched_actions,
        video_30_sec_watched_actions,
        video_avg_time_watched_actions,
        video_p100_watched_actions,
        video_p25_watched_actions,
        video_p50_watched_actions,
        video_p75_watched_actions,
        video_p95_watched_actions,
        video_play_actions,
        video_thruplay_watched_actions,
        website_ctr;

    }

    /**
     * Initializes the requestFields field using the AdAccountFields enum less the AdAccountFieldExclusion enum values
     */
    private void setRequestFields() {
        for (AdAccountFields field : AdAccountFields.values()) {
            if (!AdAccountFieldExclusion.contains(field.toString()))
                requestFields.add(field.toString());
        }
    }

    /**
     * Customizes a time range
     */
    private static class TimeRange {
        private String since;
        private String until;

        void setTimeRange(String startDate, String endDate) {
            since = startDate;
            until = endDate;
        }
    }

    /**
     * Sets the timeRangeFormatted field with a json string representing a TimeRange object
     * @param startDate The start date in 'yyyy-MM-dd' format
     * @param endDate The end date in 'yyyy-MM-dd' format
     */
    public void setTimeRangeFormatted(String startDate, String endDate) {
        TimeRange obj = new TimeRange();
        obj.setTimeRange(startDate, endDate);
        Gson timeRange = new Gson();
        timeRangeFormatted = timeRange.toJson(obj);
    }

    /**
     * Setter for the actionBreakdowns field
     */
    private void setActionBreakdowns() {
        actionBreakdowns.add(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_TYPE);
        //actionBreakdowns.add(AdsInsights.EnumActionBreakdowns.VALUE_ACTION_DEVICE);
    }

    private void setBreakdowns() {
        breakdowns.add(AdsInsights.EnumBreakdowns.VALUE_IMPRESSION_DEVICE);
        breakdowns.add(AdsInsights.EnumBreakdowns.VALUE_PUBLISHER_PLATFORM);
    }

    private void setActionAttributionWindows() {
        actionAttributionWindows.add(AdsInsights.EnumActionAttributionWindows.VALUE_1D_CLICK);
    }

    private static class CampaignWithImpression {
        private String field;
        private String operator;
        private Integer value;
        void apply() {
            field = "ad.impressions";
            operator = "GREATER_THAN";
            value = 0;
        }
    }

    private void setCampaignFilterJson() {
        CampaignWithImpression campaignWithImpression = new CampaignWithImpression();
        campaignWithImpression.apply();
        Gson campaignFilter = new Gson();
        campaignFilterJson = campaignFilter.toJson(campaignWithImpression);
    }

    public void init() {
        setActionBreakdowns();
        setBreakdowns();
        setActionAttributionWindows();
        setCampaignFilterJson();
        setRequestFields();
    }

}
