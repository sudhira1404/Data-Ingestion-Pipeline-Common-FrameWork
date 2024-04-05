package com.target.kelsaapi.common.vo.pinterest;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.compress.utils.Lists;

import java.util.List;

public class PinterestReportRequest {
    @Getter
    private final String end_date;
    @Getter
    private final String start_date;
    @Getter
    private List<String> columns;
    @Getter
    private String granularity;
    //@Getter
    //private String metrics;
    @Getter
    private String level;

    @Getter
    @Setter
    private String report_format;

    public PinterestReportRequest(String startDate, String endDate) {
        this.start_date = startDate;
        this.end_date = endDate;
        setColumns();
        setGranularity();
        //setMetrics();
        setLevel();
        setReport_format("CSV");
    }

    enum PINTEREST_REPORT_COLUMNS {
        ADVERTISER_ID,
        CAMPAIGN_ID,
        CAMPAIGN_NAME,
        CAMPAIGN_STATUS,
        CLICKTHROUGH_1,
        CLICKTHROUGH_1_GROSS,
        CLICKTHROUGH_2,
        TOTAL_CLICKTHROUGH,
        OUTBOUND_CLICK_1,
        OUTBOUND_CLICK_2,
        ECTR,//CTR
        ECPC_IN_DOLLAR,//CPC
        CPM_IN_DOLLAR,//CPM
        ECPCV_IN_DOLLAR,//CPCV (100%)
        ECPCV_P95_IN_DOLLAR,//CPCV (95%)
        ECPV_IN_DOLLAR,//CPV
        SPEND_IN_DOLLAR,//SPEND
        ENGAGEMENT_1,
        ENGAGEMENT_2,
        TOTAL_ENGAGEMENT,
        IMPRESSION_1,
        IMPRESSION_1_GROSS,
        IMPRESSION_2,
        TOTAL_IMPRESSION_FREQUENCY,//Frequency
        TOTAL_IMPRESSION_USER,//Reach
        REPIN_1,
        REPIN_2,
        TOTAL_REPIN_RATE,//Total save rate
        VIDEO_AVG_WATCHTIME_IN_SECOND_1,
        VIDEO_AVG_WATCHTIME_IN_SECOND_2,
        TOTAL_VIDEO_AVG_WATCHTIME_IN_SECOND,//Average Watch Time In Seconds
        VIDEO_MRC_VIEWS_1,
        VIDEO_MRC_VIEWS_2,
        TOTAL_VIDEO_MRC_VIEWS,//Video views
        VIDEO_P0_COMBINED_1,
        VIDEO_P0_COMBINED_2,
        TOTAL_VIDEO_P0_COMBINED,//Total video starts
        VIDEO_P100_COMPLETE_1,
        VIDEO_P100_COMPLETE_2,
        TOTAL_VIDEO_P100_COMPLETE,//Total video played at 100%
        VIDEO_P25_COMBINED_1,
        VIDEO_P25_COMBINED_2,
        TOTAL_VIDEO_P25_COMBINED,//Total video played at 25%
        VIDEO_P50_COMBINED_1,
        VIDEO_P50_COMBINED_2,
        TOTAL_VIDEO_P50_COMBINED,//Total video played at 50%
        VIDEO_P75_COMBINED_1,
        VIDEO_P75_COMBINED_2,
        TOTAL_VIDEO_P75_COMBINED,//Total video played at 75%
        VIDEO_P95_COMBINED_1,
        VIDEO_P95_COMBINED_2,
        TOTAL_VIDEO_P95_COMBINED//Total video played at 95%
    }

    private void setColumns() {

        List<String> thisList = Lists.newArrayList();
        for (PINTEREST_REPORT_COLUMNS column : PINTEREST_REPORT_COLUMNS.values()) {
            thisList.add(column.toString());
        }
        this.columns = thisList;
    }

    private void setGranularity() {
        this.granularity = "DAY";
    }
    /*private void setMetrics() {
        this.metrics = "ALL";
    }*/

    private void setLevel() {
        this.level = "CAMPAIGN";
    }
}
