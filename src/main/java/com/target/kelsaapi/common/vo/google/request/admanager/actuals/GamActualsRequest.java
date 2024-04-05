package com.target.kelsaapi.common.vo.google.request.admanager.actuals;

import com.google.api.ads.admanager.axis.v202311.*;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.util.Arrays;


/**
 *
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class GamActualsRequest extends GamRequest {

    private ReportQuery reportQueryObject;

    private Column[] reportColumns;

    private Dimension[] reportDimensions;

    private DimensionAttribute[] reportDimensionAttributes;

    private enum GamReportDimensions {
        DATE,
        LINE_ITEM_ID,
        LINE_ITEM_NAME,
        LINE_ITEM_TYPE,
        POSTAL_CODE_CRITERIA_ID,
        POSTAL_CODE,
        ORDER_ID,
        ORDER_NAME
    }

    private enum GamReportColumns {
        AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS,//int
        AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS,//int
        AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE,//micro currency
        AD_SERVER_IMPRESSIONS,//int
        AD_SERVER_CLICKS,//int
        AD_SERVER_WITHOUT_CPD_AVERAGE_ECPM,//micro currency / 1000
        AD_SERVER_CTR,//double/decimal/float
        AD_SERVER_CPM_AND_CPC_REVENUE,//micro currency
        TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS,//int
        TOTAL_LINE_ITEM_LEVEL_CLICKS,//int
        TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE,//micro currency
        TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM,//micro currency / 1000
        TOTAL_LINE_ITEM_LEVEL_CTR//double/decimal/float
    }

    private enum GamReportDimensionAttributes {
        LINE_ITEM_START_DATE_TIME,
        LINE_ITEM_END_DATE_TIME,
        LINE_ITEM_LIFETIME_IMPRESSIONS,
        LINE_ITEM_LIFETIME_CLICKS,
        LINE_ITEM_DELIVERY_INDICATOR,
        LINE_ITEM_DELIVERY_PACING,
        ORDER_PO_NUMBER,
        ORDER_START_DATE_TIME,
        ORDER_END_DATE_TIME,
        ORDER_LIFETIME_IMPRESSIONS,
        ORDER_LIFETIME_CLICKS
    }

    public GamActualsRequest(String start, String end) throws ParseException {
        super(start, end);
        setReportColumns();
        setReportDimensions();
        setReportDimensionAttributes();
        setReportQueryObject(getStartDate(), getEndDate());
    }

    public GamActualsRequest(String start, String end, String zipCode) throws ParseException {
        super(start, end);
        setReportColumns();
        setReportDimensions();
        setReportDimensionAttributes();
        setReportQueryObject(getStartDate(), getEndDate(), zipCode);
    }

    private void setReportColumns() {

        Column[] columns = new Column[GamReportColumns.values().length];
        int index = 0;
        for (GamReportColumns column : GamReportColumns.values()) {
            columns[index] = Column.fromString(column.toString());
            index++;
        }

        this.reportColumns = columns;
    }

    private void setReportDimensions() {

        Dimension[] dimensions = new Dimension[GamReportDimensions.values().length];
        int index = 0;
        for (GamReportDimensions dimension : GamReportDimensions.values()) {
            dimensions[index] = Dimension.fromString(dimension.toString());
            index++;
        }

        this.reportDimensions = dimensions;
    }

    private void setReportDimensionAttributes() {

        DimensionAttribute[] attributes = new DimensionAttribute[GamReportDimensionAttributes.values().length];
        int index = 0;
        for (GamReportDimensionAttributes attribute : GamReportDimensionAttributes.values()) {
            attributes[index] = DimensionAttribute.fromString(attribute.toString());
            index++;
        }

        this.reportDimensionAttributes = attributes;
    }

    private void setReportQueryObject(String startDate, String endDate) throws ParseException {
        setReportQueryObject(startDate, endDate,null);
    }

    private void setReportQueryObject(String startDate, String endDate, @Nullable String zipCode) throws ParseException {
        ReportQuery reportQuery = new ReportQuery();

        reportQuery.setAdUnitView(ReportQueryAdUnitView.TOP_LEVEL);
        reportQuery.setDimensions(reportDimensions);
        reportQuery.setDimensionAttributes(reportDimensionAttributes);
        reportQuery.setColumns(reportColumns);
        reportQuery.setDateRangeType(DateRangeType.CUSTOM_DATE);
        reportQuery.setStartDate(toDate(startDate));
        reportQuery.setEndDate(toDate(endDate));
        log.info("Report query Ad Unit View : {}", reportQuery.getAdUnitView());
        log.info("Report query Dimensions : {}", Arrays.toString(reportQuery.getDimensions()));
        log.info("Report query Columns : {}", Arrays.toString(reportQuery.getColumns()));
        log.info("Report query Date Range Type : {}", reportQuery.getDateRangeType().getValue());
        log.info("Report query Start Date : {}", reportQuery.getStartDate().toString());
        log.info("Report query End Date : {}", reportQuery.getEndDate().toString());
        if (zipCode == null) {
            reportQuery.setStatement(formatStatement());
        } else {
            reportQuery.setStatement(formatStatement(zipCode));
        }
        log.info("Report query Statement : {}", reportQuery.getStatement().getQuery());
        this.reportQueryObject =  reportQuery;
    }

    private Statement formatStatement(String zipCode) {
        Statement statement = new Statement();
        //Filter to pull subset of data from api
        String query = "where POSTAL_CODE IN (" + zipCode + ")";
        statement.setQuery(query);
        return statement;
    }

    private Statement formatStatement() {
        Statement statement = new Statement();
        //Filter to pull subset of data from api
        String query = ApplicationConstants.EMPTY_POSTAL_CODE_GAM_QUERY;
        statement.setQuery(query);
        return statement;
    }

}
