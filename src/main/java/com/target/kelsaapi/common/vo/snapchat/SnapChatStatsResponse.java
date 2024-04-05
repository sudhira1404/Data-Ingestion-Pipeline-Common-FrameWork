package com.target.kelsaapi.common.vo.snapchat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

@Data
@Slf4j
public class SnapChatStatsResponse {

    private ArrayList<String> stats;

    public SnapChatStatsResponse(String response, String adAccountId, String adAccountName) {
        ArrayList<TimeseriesParentStat> statsResponses = responseToSnapChatStatsList(response);
        this.stats = decorateStats(statsResponses, adAccountId, adAccountName);
        log.debug("Total number of campaign + dma level stats fetched from this request: {}", this.stats.size());
    }

    private ArrayList<TimeseriesParentStat> responseToSnapChatStatsList(String response) {
        Gson gson = new GsonBuilder().create();
        StatsRequest request = gson.fromJson(response, StatsRequest.class);
        return new ArrayList<>(Arrays.asList(request.getTimeseries_stats()));
    }

    private ArrayList<String> decorateStats(ArrayList<TimeseriesParentStat> stats, String adAccountId, String adAccountName) {
        ArrayList<String> returnableStatsList = new ArrayList<>();
        stats.forEach(parentStat -> {
            TimeseriesStat timeseriesStat = parentStat.getTimeseries_stat();
            ArrayList<Campaign> campaignStats = new ArrayList<>(Arrays.asList(timeseriesStat.getBreakdown_stats().getCampaign()));
            campaignStats.forEach(campaign -> {
                ArrayList<Timeseries> timeseries = new ArrayList<>(Arrays.asList(campaign.getTimeseries()));
                timeseries.forEach(time -> {
                    ArrayList<DimensionStats> dimensionStats = new ArrayList<>(Arrays.asList(time.getDimension_stats()));
                    dimensionStats.forEach(dimension -> {
                        DecoratedStats decoratedStats = new DecoratedStats(dimension, campaign, timeseriesStat, adAccountId, adAccountName);
                        Gson gson = new Gson();
                        returnableStatsList.add(gson.toJson(decoratedStats));
                    });
                });
            });
        });
        return returnableStatsList;
    }


    @Data
    public class DecoratedStats {
        //Campaign
        private String campaign_granularity;
        private String campaign_id; //renamed from id

        //Ad Account
        private String ad_account_id;
        private String ad_account_name;

        //DimensionStats
        private long attachment_total_view_time_millis;
        private long conversion_add_billing;
        private long conversion_add_cart;
        private long conversion_app_opens;
        private long conversion_level_completes;
        private long conversion_page_views;
        private long conversion_purchases;
        private long conversion_purchases_value;
        private long conversion_save;
        private long conversion_searches;
        private long conversion_sign_ups;
        private long conversion_start_checkout;
        private long conversion_view_content;
        private String dma;
        private long impressions;
        private long quartile_1;//nullable in source
        private long quartile_2;//nullable in source
        private long quartile_3;//nullable in source
        private long screen_time_millis;
        private long shares;
        private long spend;
        private long swipes;
        private long total_installs;
        private long video_views;//nullable in source
        private long view_completion;//nullable in source

        //Campaign
        private String campaign_type; //renamed from type
        private String campaign_start_time;
        private String campaign_end_time;

        //TimeseriesStat
        private String timeseries_stat_swipe_up_attribution_window; //rename from swipe_up_attribution_window
        private String timeseries_stat_view_attribution_window; //rename from view_attribution_window

        public DecoratedStats(DimensionStats dimension, Campaign campaign, TimeseriesStat timeseriesStat, String adAccountId, String adAccountName) {
            this.campaign_granularity = campaign.getGranularity();
            this.campaign_id = campaign.getId();
            this.ad_account_id = adAccountId;
            this.ad_account_name = adAccountName;
            this.attachment_total_view_time_millis = dimension.getAttachment_total_view_time_millis();
            this.conversion_add_billing = dimension.getConversion_add_billing();
            this.conversion_add_cart = dimension.getConversion_add_cart();
            this.conversion_app_opens = dimension.getConversion_app_opens();
            this.conversion_level_completes = dimension.getConversion_level_completes();
            this.conversion_page_views = dimension.getConversion_page_views();
            this.conversion_purchases = dimension.getConversion_purchases();
            this.conversion_purchases_value = dimension.getConversion_purchases_value();
            this.conversion_save = dimension.getConversion_save();
            this.conversion_searches = dimension.getConversion_searches();
            this.conversion_sign_ups = dimension.getConversion_sign_ups();
            this.conversion_start_checkout = dimension.getConversion_start_checkout();
            this.conversion_view_content = dimension.getConversion_view_content();
            this.dma = dimension.getDma();
            this.impressions = dimension.getImpressions();
            this.quartile_1 = Objects.requireNonNullElse(dimension.getQuartile_1(),0L);
            this.quartile_2 = Objects.requireNonNullElse(dimension.getQuartile_2(), 0L);
            this.quartile_3 = Objects.requireNonNullElse(dimension.getQuartile_3(), 0L);
            this.screen_time_millis = dimension.getScreen_time_millis();
            this.shares = dimension.getShares();
            this.spend = dimension.getSpend();
            this.swipes = dimension.getSwipes();
            this.total_installs = dimension.getTotal_installs();
            this.video_views = Objects.requireNonNullElse(dimension.getVideo_views(), 0L);
            this.view_completion = Objects.requireNonNullElse(dimension.getView_completion(), 0L);
            this.campaign_type = campaign.getType();
            this.campaign_end_time = campaign.getEnd_time();
            this.campaign_start_time = campaign.getStart_time();
            this.timeseries_stat_swipe_up_attribution_window = timeseriesStat.getSwipe_up_attribution_window();
            this.timeseries_stat_view_attribution_window = timeseriesStat.getView_attribution_window();
        }

    }

    @Data
    public static class StatsRequest {
        private String request_status;
        private String request_id;
        private TimeseriesParentStat[] timeseries_stats;
    }


    @Data
    public static class TimeseriesParentStat {
        private String sub_request_status;
        private TimeseriesStat timeseries_stat;

    }

    @Data
    public static class TimeseriesStat {
        private String id;
        private String type;
        //This is the swipe_up_attribution_window requested from the API via query parameter
        private String swipe_up_attribution_window;
        //This is the view_attribution_window requested from the API via query parameter
        private String view_attribution_window;
        private String start_time;
        private String end_time;
        private String finalized_data_end_time;
        private String conversion_data_processed_end_time;
        private Paging paging;
        private BreakdownStats breakdown_stats;
    }

    @Data
    public static class BreakdownStats {
        //This is the breakdown requested from the API via query parameter
        private Campaign[] campaign;
    }

    @Data
    public static class Campaign {
        private String id;
        private String type;
        //This is the granularity requested from the API via query parameter
        private String granularity;
        //This is the start_time requested from the API via query parameter
        private String start_time;
        //This is the end_time requested from the API via query parameter
        private String end_time;
        private Timeseries[] timeseries;
    }

    @Data
    public static class Timeseries {
        private String start_time;
        private String end_time;
        private DimensionStats[] dimension_stats;
    }

    @Data
    public static class DimensionStats {
        //These are the fields requested from the API via query parameter
        private long impressions;
        private long swipes;
        private long quartile_1;
        private long quartile_2;
        private long quartile_3;
        private long view_completion;
        private long attachment_total_view_time_millis;
        private long spend;
        private long video_views;
        private long shares;
        private long total_installs;
        private long conversion_purchases;
        private long conversion_purchases_value;
        private long conversion_save;
        private long conversion_start_checkout;
        private long conversion_add_cart;
        private long conversion_view_content;
        private long conversion_add_billing;
        private long conversion_sign_ups;
        private long conversion_searches;
        private long conversion_level_completes;
        private long conversion_app_opens;
        private long conversion_page_views;
        private long screen_time_millis;

        //This is the report_dimension requested from the API via query parameter
        private String dma;

    }
}