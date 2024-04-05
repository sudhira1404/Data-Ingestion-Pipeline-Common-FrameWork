package com.target.kelsaapi.common.vo.google.response.admanager.delivery;

import com.google.api.ads.admanager.axis.v202311.Order;
import com.google.gson.ExclusionStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.target.kelsaapi.common.util.GamUtils;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

public class GamOrder extends Order {

    @Getter
    @Setter
    private String reportDate;

    @Setter
    @Getter
    private String reportCreateTimestamp;

    @Setter
    @SerializedName("orderEndDateTime")
    private String endDateTime;

    @Setter
    @SerializedName("orderLastModifiedDateTime")
    private String lastModifiedDateTime;

    @Getter
    private String gamRecordType = "ORDER";

    @Setter
    @SerializedName("orderStartDateTime")
    private String startDateTime;

    @Setter
    @SerializedName("orderStatus")
    private String status;

    @Getter
    @Setter
    private String totalBudgetCurrencyCode;

    @Getter
    @Setter
    private long totalBudgetMicroAmount;

    public GamOrder(Order order, String reportDate, Instant reportTimestamp) {

        // Copy all the fields from the Order object
        this.setAdvertiserContactIds(order.getAdvertiserContactIds());
        this.setAdvertiserId(order.getAdvertiserId());
        this.setAgencyContactIds(order.getAgencyContactIds());
        this.setAgencyId(order.getAgencyId());
        this.setAppliedLabels(order.getAppliedLabels());
        this.setAppliedTeamIds(order.getAppliedTeamIds());
        this.setCreatorId(order.getCreatorId());
        this.setCurrencyCode(order.getCurrencyCode());
        this.setCustomFieldValues(order.getCustomFieldValues());
        this.setEffectiveAppliedLabels(order.getEffectiveAppliedLabels());
        this.setExternalOrderId(order.getExternalOrderId());
        this.setId(order.getId());
        this.setIsArchived(order.getIsArchived());
        this.setIsProgrammatic(order.getIsProgrammatic());
        this.setLastModifiedByApp(order.getLastModifiedByApp());
        this.setName(order.getName());
        this.setNotes(order.getNotes());
        this.setPoNumber(order.getPoNumber());
        this.setSalespersonId(order.getSalespersonId());
        this.setSecondarySalespersonIds(order.getSecondarySalespersonIds());
        this.setSecondaryTraffickerIds(order.getSecondaryTraffickerIds());
        this.setTotalClicksDelivered(order.getTotalClicksDelivered());
        this.setTotalImpressionsDelivered(order.getTotalImpressionsDelivered());
        this.setTotalViewableImpressionsDelivered(order.getTotalViewableImpressionsDelivered());
        this.setTraffickerId(order.getTraffickerId());
        this.setUnlimitedEndDateTime(order.getUnlimitedEndDateTime());

        // Set derived/overridden fields
        setReportDate(reportDate);
        setEndDateTime(GamUtils.dateTimeToString(order.getEndDateTime()));
        setLastModifiedDateTime(GamUtils.dateTimeToString(order.getLastModifiedDateTime()));
        setStartDateTime(GamUtils.dateTimeToString(order.getStartDateTime()));
        setStatus(order.getStatus().toString());
        setTotalBudgetCurrencyCode(order.getTotalBudget().getCurrencyCode());
        setTotalBudgetMicroAmount(order.getTotalBudget().getMicroAmount());
        setReportCreateTimestamp(GamUtils.instantToCentralZoneString(reportTimestamp));
    }

    @Override
    public String toString() {
        return com.google.common.base.MoreObjects.toStringHelper(this.getClass())
                .add("advertiserContactIds", getAdvertiserContactIds())
                .add("advertiserId", getAdvertiserId())
                .add("agencyContactIds", getAgencyContactIds())
                .add("agencyId", getAgencyId())
                .add("appliedLabels", getAppliedLabels())
                .add("appliedTeamIds", getAppliedTeamIds())
                .add("creatorId", getCreatorId())
                .add("currencyCode", getCurrencyCode())
                .add("customFieldValues", getCustomFieldValues())
                .add("effectiveAppliedLabels", getEffectiveAppliedLabels())
                .add("endDateTime", getEndDateTimeAsString())
                .add("externalOrderId", getExternalOrderId())
                .add("id", getId())
                .add("isArchived", getIsArchived())
                .add("isProgrammatic", getIsProgrammatic())
                .add("lastModifiedByApp", getLastModifiedByApp())
                .add("lastModifiedDateTime", getLastModifiedDateTimeAsString())
                .add("name", getName())
                .add("notes", getNotes())
                .add("poNumber", getPoNumber())
                .add("salespersonId", getSalespersonId())
                .add("secondarySalespersonIds", getSecondarySalespersonIds())
                .add("secondaryTraffickerIds", getSecondaryTraffickerIds())
                .add("startDateTime", getStartDateTimeAsString())
                .add("status", getStatusAsString())
                .add("totalBudgetCurrencyCode", getTotalBudgetCurrencyCode())
                .add("totalBudgetMicroAmount", getTotalBudgetMicroAmount())
                .add("totalClicksDelivered", getTotalClicksDelivered())
                .add("totalImpressionsDelivered", getTotalImpressionsDelivered())
                .add("totalViewableImpressionsDelivered", getTotalViewableImpressionsDelivered())
                .add("traffickerId", getTraffickerId())
                .add("unlimitedEndDateTime", getUnlimitedEndDateTime())
                .add("reportDate", getReportDate())
                .toString();
    }

    public String getEndDateTimeAsString() { return this.endDateTime; }

    public String getStartDateTimeAsString() { return this.startDateTime; }

    public String getLastModifiedDateTimeAsString() { return this.lastModifiedDateTime; }

    public String getStatusAsString() { return this.status; }

    public String toJson() {
        ExclusionStrategy excludeSuperClass = new GamJsonSerializationExclusionStrategy(Order.class);
        Gson gson = new GsonBuilder()
                .setExclusionStrategies(excludeSuperClass)
                .create();
        return gson.toJson(this);
    }

}
