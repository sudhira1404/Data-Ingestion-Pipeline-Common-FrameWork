package com.target.kelsaapi.common.vo.google.request.marketingplatform;

import com.google.api.client.util.DateTime;
import com.google.api.client.util.Lists;
import com.google.api.services.dfareporting.DfareportingScopes;
import com.google.api.services.dfareporting.model.DateRange;
import com.google.api.services.dfareporting.model.Report;
import com.google.api.services.dfareporting.model.SortedDimension;
import com.google.common.collect.ImmutableSet;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Data object used to instantiate a new report request for Campaign Manager 360
 */
@EqualsAndHashCode(callSuper=false)
@Data
public class CampaignManager360ReportRequest extends MarketingPlatformRequest {
    private final ImmutableSet<String> OAUTH_SCOPES = ImmutableSet.of(DfareportingScopes.DFAREPORTING);
    private final String REPORT_TYPE = "STANDARD";
    private final String REPORT_FORMAT = "CSV";

    private DateRange dateRange;
    private ArrayList<SortedDimension> dimensions;
    private List<String> metrics;
    private Report report;


    /**
     * Constuctor for instantiating a new Campaign Manager 360 report request
     *
     * @param startDate The start date in yyyy-mm-dd format
     * @param endDate The end date in yyyy-MM-dd format
     */
    public CampaignManager360ReportRequest(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
        setReport();
    }

    private void setReport() {
        String reportName = APPLICATION_NAME + "-" + startDate + "-" + endDate;
        this.report = new Report();
        // Set the required fields "name" and "type".
        report.setName(reportName);
        report.setType(REPORT_TYPE);

        // Set optional fields
        report.setFileName(reportName);
        report.setFormat(REPORT_FORMAT);

        // Set report criteria
        Report.Criteria criteria = new Report.Criteria();
        setDateRange();
        criteria.setDateRange(dateRange);
        setDimensions();
        criteria.setDimensions(dimensions);
        setMetrics();
        criteria.setMetricNames(metrics);
        report.setCriteria(criteria);

    }

    private void setDateRange() {
        this.dateRange = new DateRange();
        dateRange.setEndDate(DateTime.parseRfc3339(endDate));
        dateRange.setStartDate(DateTime.parseRfc3339(startDate));
    }

    private void setDimensions() {
        this.dimensions = Lists.newArrayList();
        //dimension names courtesy of: https://developers.google.com/doubleclick-advertisers/v3.4/dimensions#standard-dimensions
        List<String> dimensionNames = List.of(
                "campaign",
                "campaignId",
                "campaignExternalId",
                "campaignStartDate",
                "campaignEndDate",
                "platformType",
                "site",
                "siteId",
                "siteDirectory",
                "siteDirectoryId",
                "siteKeyname",
                "date",
                "zipCode",
                "creativeSize",
                "packageRoadblock",
                "packageRoadblockId",
                "placementId",
                "placement"
        );
        for (String name: dimensionNames) {
            dimensions.add(new SortedDimension().setName(name));
        }
    }

    private void setMetrics() {
        //List courtesy of: https://developers.google.com/doubleclick-advertisers/v3.4/dimensions#standard-metrics
        this.metrics = List.of(
                "impressions",
                "activeViewViewableImpressions",
                "activeViewPercentageViewableImpressions",
                "activeViewMeasurableImpressions",
                "activeViewPercentageMeasurableImpressions",
                "activeViewEligibleImpressions",
                "clicks",
                "costPerActivity",
                "costPerClick",
                "costPerRevenue",
                "mediaCost",
                "plannedMediaCost",
                "richMediaVideoPlays",
                "richMediaVideoFirstQuartileCompletes",
                "richMediaVideoMidpoints",
                "richMediaVideoThirdQuartileCompletes",
                "richMediaVideoCompletions",
                "richMediaAverageVideoViewTime"
        );
    }

}
