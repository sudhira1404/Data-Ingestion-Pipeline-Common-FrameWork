package com.target.kelsaapi.common.vo.google.response.admanager.delivery;

import com.google.api.ads.admanager.axis.v202311.*;
import com.google.api.client.util.Lists;
import com.google.gson.ExclusionStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.target.kelsaapi.common.util.GamUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.List;

@Slf4j
public class GamLineItem extends LineItem {

    @Setter
    @Getter
    private double actualDeliveryPercentage;

    @Getter
    @SerializedName("lineItemAllowedFormats")
    private List<String> _allowedFormats;

    @Getter
    @Setter
    @SerializedName("lineItemAppliedLabels")
    private List<GamAppliedLabel> _appliedLabels;

    @Getter
    @Setter
    private String budgetCurrencyCode;

    @Getter
    @Setter
    private long budgetMicroAmount;

    @Getter
    @Setter
    @SerializedName("lineItemChildContentEligibility")
    private String _childContentEligibility;

    @Setter
    @Getter
    private long clicksDelivered;

    @Getter
    @Setter
    @SerializedName("lineItemCompanionDeliveryOption")
    private String _companionDeliveryOption;

    @Getter
    @Setter
    @SerializedName("lineItemCompetitiveConstraintScope")
    private String _competitiveConstraintScope;

    @Getter
    @Setter
    private String costPerUnitCurrencyCode;

    @Getter
    @Setter
    private long costPerUnitMicroAmount;

    @Getter
    @Setter
    @SerializedName("lineItemCostType")
    private String _costType;

    @Setter
    @SerializedName("lineItemCreationDateTime")
    private String creationDateTime;

    @Getter
    @Setter
    @SerializedName("lineItemCreativeRotationType")
    private String _creativeRotationType;

    @Getter
    @Setter
    private List<Long> customFieldValueIds;

    @Getter
    protected List<GamCustomPacingGoal> customPacingGoals;

    @Setter
    @Getter
    private long[] deliveryDataUnits;

    @Getter
    @Setter
    @SerializedName("lineItemDeliveryForecastSource")
    private String _deliveryForecastSource;

    @Getter
    @Setter
    @SerializedName("lineItemDeliveryRateType")
    private String _deliveryRateType;

    @Getter
    @Setter
    @SerializedName("lineItemDiscountType")
    private String _discountType;

    @Getter
    @Setter
    @SerializedName("lineItemEffectiveAppliedLabels")
    private List<GamAppliedLabel> _effectiveAppliedLabels;

    @Setter
    @SerializedName("lineItemEndDateTime")
    private String endDateTime;

    @Getter
    @Setter
    @SerializedName("lineItemEnvironmentType")
    private String _environmentType;

    @Setter
    @Getter
    private double expectedDeliveryPercentage;

    @Getter
    @Setter
    @SerializedName("lineItemExternalDealId")
    private long externalDealId;

    @Getter
    private String gamRecordType = "LINE_ITEM";

    @Getter
    @Setter
    @SerializedName("lineItemGroupSettings")
    private GamGroupSettings groupSettings;

    @Setter
    @Getter
    private long impressionsDelivered;

    @Setter
    @SerializedName("lineItemLastModifiedDateTime")
    private String lastModifiedDateTime;

    @Getter
    protected List<GamActivityAssociation> lineItemActivityAssociations;

    @Getter
    protected List<GamCreativePlaceholder> lineItemCreativePlaceholders;

    @Getter
    protected List<GamFrequencyCap> lineItemFrequencyCaps;

    @Getter
    @Setter
    @SerializedName("lineItemTypeValue")
    private String _lineItemType;

    @Getter
    @Setter
    @SerializedName("lineItemPrimaryGoal")
    private GamGoal primaryGoal;

    @Getter
    @Setter
    @SerializedName("lineItemProgrammaticCreativeSource")
    private String _programmaticCreativeSource;

    @Getter
    @Setter
    private String reportDate;

    @Setter
    @Getter
    private String reportCreateTimestamp;

    @Getter
    @Setter
    @SerializedName("lineItemReservationStatus")
    private String _reservationStatus;

    @Getter
    @Setter
    @SerializedName("lineItemRoadBlockingType")
    private String _roadBlockingType;

    @Getter
    @Setter
    @SerializedName("lineItemSecondaryGoals")
    private List<GamGoal> _secondaryGoals;

    @Getter
    @Setter
    @SerializedName("lineItemSkippableAdType")
    private String _skippableAdType;

    @Setter
    @SerializedName("lineItemStartDateTime")
    private String startDateTime;

    @Getter
    @Setter
    @SerializedName("lineItemStartDateTimeType")
    private String _startDateTimeType;

    @Getter
    @Setter
    @SerializedName("lineItemStatus")
    private String _status;

    @Getter
    @Setter
    @SerializedName("lineItemThirdPartyMeasurementSettings")
    private GamThirdPartyMeasurementSettings thirdPartyMeasurementSettings;

    @Getter
    @Setter
    private String valueCostPerUnitCurrencyCode;

    @Getter
    @Setter
    private long valueCostPerUnitMicroAmount;

    @Setter
    @Getter
    private long videoCompletionsDelivered;

    @Setter
    @Getter
    private long videoStartsDelivered;

    @Setter
    @Getter
    private long viewableImpressionsDelivered;


    public GamLineItem(LineItem lineItem, String reportDate, Instant reportTimestamp) {

        // Copy all the fields from the Line Item object
        setOrderId(lineItem.getOrderId());
        setId(lineItem.getId());
        setName(lineItem.getName());
        setExternalId(lineItem.getExternalId());
        setOrderName(lineItem.getOrderName());
        setAutoExtensionDays(lineItem.getAutoExtensionDays());
        setUnlimitedEndDateTime(lineItem.getUnlimitedEndDateTime());
        setPriority(lineItem.getPriority());
        setDiscount(lineItem.getDiscount());
        setContractedUnitsBought(lineItem.getContractedUnitsBought());
        setAllowOverbook(lineItem.getAllowOverbook());
        setSkipInventoryCheck(lineItem.getSkipInventoryCheck());
        setSkipCrossSellingRuleWarningChecks(lineItem.getSkipCrossSellingRuleWarningChecks());
        setReserveAtCreation(lineItem.getReserveAtCreation());
        setIsArchived(lineItem.getIsArchived());
        setWebPropertyCode(lineItem.getWebPropertyCode());
        setDisableSameAdvertiserCompetitiveExclusion(lineItem.getDisableSameAdvertiserCompetitiveExclusion());
        setLastModifiedByApp(lineItem.getLastModifiedByApp());
        setNotes(lineItem.getNotes());
        setIsMissingCreatives(lineItem.getIsMissingCreatives());
        setVideoMaxDuration(lineItem.getVideoMaxDuration());
        setViewabilityProviderCompanyIds(lineItem.getViewabilityProviderCompanyIds());
        setCustomVastExtension(lineItem.getCustomVastExtension());
        setTargeting(lineItem.getTargeting());
        setCreativeTargetings(lineItem.getCreativeTargetings());
        log.debug("Copied all un-modified fields. Attempting to transform the remaining fields now...");

        // Set objects that only have a single _value field to String fields
        ChildContentEligibility cce = lineItem.getChildContentEligibility();
        if (cce != null) this.set_childContentEligibility(cce.getValue());

        CompanionDeliveryOption cdo = lineItem.getCompanionDeliveryOption();
        if (cdo != null) this.set_companionDeliveryOption(cdo.getValue());

        CompetitiveConstraintScope ccs = lineItem.getCompetitiveConstraintScope();
        if (ccs != null) this.set_competitiveConstraintScope(ccs.getValue());

        StartDateTimeType sdtt = lineItem.getStartDateTimeType();
        if (sdtt != null) this.set_startDateTimeType(sdtt.getValue());

        CreativeRotationType crt = lineItem.getCreativeRotationType();
        if (crt != null) this.set_creativeRotationType(crt.getValue());

        DeliveryRateType drt = lineItem.getDeliveryRateType();
        if (drt != null) this.set_deliveryRateType(drt.getValue());

        DeliveryForecastSource dfs = lineItem.getDeliveryForecastSource();
        if (dfs != null) this.set_deliveryForecastSource(dfs.getValue());

        RoadblockingType rbt = lineItem.getRoadblockingType();
        if (rbt != null) this.set_roadBlockingType(rbt.getValue());

        SkippableAdType sat = lineItem.getSkippableAdType();
        if (sat != null) this.set_skippableAdType(sat.getValue());

        LineItemType lit = lineItem.getLineItemType();
        if (lit != null) this.set_lineItemType(lit.getValue());

        CostType ct = lineItem.getCostType();
        if (ct != null) this.set_costType(ct.getValue());

        LineItemDiscountType dt = lineItem.getDiscountType();
        if (dt != null) this.set_discountType(dt.getValue());

        EnvironmentType et = lineItem.getEnvironmentType();
        if (et != null) this.set_environmentType(et.getValue());

        ComputedStatus compst = lineItem.getStatus();
        if (compst != null) this.set_status(compst.getValue());

        LineItemSummaryReservationStatus rs = lineItem.getReservationStatus();
        if (rs != null) this.set_reservationStatus(rs.getValue());

        ProgrammaticCreativeSource pcs = lineItem.getProgrammaticCreativeSource();
        if (pcs != null) this.set_programmaticCreativeSource(pcs.getValue());
        log.debug("All fields derived from a getValue() call to an object are initialized. " +
                "Moving on to complex field transformations");


        // Set derived/overridden fields
        this.setReportDate(reportDate);
        this.setStartDateTime(GamUtils.dateTimeToString(lineItem.getStartDateTime()));
        this.setEndDateTime(GamUtils.dateTimeToString(lineItem.getEndDateTime()));
        this.setGamCustomPacingGoals(lineItem.getCustomPacingCurve());
        this.setLineItemFrequencyCaps(lineItem.getFrequencyCaps());
        this.setReportCreateTimestamp(GamUtils.instantToCentralZoneString(reportTimestamp));

        Money cpu = lineItem.getCostPerUnit();
        if (cpu != null) {
            this.setCostPerUnitCurrencyCode(cpu.getCurrencyCode());
            this.setCostPerUnitMicroAmount(cpu.getMicroAmount());
        }

        Money vcpu = lineItem.getValueCostPerUnit();
        if (vcpu != null) {
            this.setValueCostPerUnitCurrencyCode(vcpu.getCurrencyCode());
            this.setValueCostPerUnitMicroAmount(vcpu.getMicroAmount());
        }
        this.setLineItemCreativePlaceholders(lineItem.getCreativePlaceholders());
        this.setLineItemActivityAssociations(lineItem.getActivityAssociations());
        this.setGamAllowedFormats(lineItem.getAllowedFormats());

        Stats stats = lineItem.getStats();
        if (stats != null) {
            this.setImpressionsDelivered(stats.getImpressionsDelivered());
            this.setClicksDelivered(stats.getClicksDelivered());
            this.setVideoCompletionsDelivered(stats.getVideoCompletionsDelivered());
            this.setVideoStartsDelivered(stats.getVideoStartsDelivered());
            this.setViewableImpressionsDelivered(stats.getViewableImpressionsDelivered());
        }

        DeliveryIndicator di = lineItem.getDeliveryIndicator();
        if (di != null) {
            this.setExpectedDeliveryPercentage(di.getExpectedDeliveryPercentage());
            this.setActualDeliveryPercentage(di.getActualDeliveryPercentage());
        }

        DeliveryData dd = lineItem.getDeliveryData();
        if (dd != null) this.setDeliveryDataUnits(dd.getUnits());

        this.setBudgetCurrencyCode(lineItem.getBudget().getCurrencyCode());
        this.setBudgetMicroAmount(lineItem.getBudget().getMicroAmount());

        AppliedLabel[] al = lineItem.getAppliedLabels();
        if (al != null) this.set_appliedLabels(convertLabelList(al));

        AppliedLabel[] el = lineItem.getEffectiveAppliedLabels();
        if (el != null) this.set_effectiveAppliedLabels(convertLabelList(lineItem.getEffectiveAppliedLabels()));

        this.setLastModifiedDateTime(GamUtils.dateTimeToString(lineItem.getLastModifiedDateTime()));
        this.setCreationDateTime(GamUtils.dateTimeToString(lineItem.getCreationDateTime()));

        BaseCustomFieldValue[] cfv = lineItem.getCustomFieldValues();
        if (cfv != null) this.setCustomFieldValueIds(convertCustomValuesLongList(cfv));

        ThirdPartyMeasurementSettings tpms = lineItem.getThirdPartyMeasurementSettings();
        if (tpms != null) this.setThirdPartyMeasurementSettings(new GamThirdPartyMeasurementSettings(tpms));

        Goal pg = lineItem.getPrimaryGoal();
        if (pg != null) this.setPrimaryGoal(new GamGoal(pg));

        Goal[] sg = lineItem.getSecondaryGoals();
        if (sg != null) this.set_secondaryGoals(convertGoalsToList(sg));

        GrpSettings gs = lineItem.getGrpSettings();
        if (gs != null) this.setGroupSettings(new GamGroupSettings(gs));

        LineItemDealInfoDto didto = lineItem.getDealInfo();
        if (didto != null) this.setExternalDealId(lineItem.getDealInfo().getExternalDealId());

        log.debug("Finished initializing GamLineItem object");

    }

    @Override
    public String toString() {
        return com.google.common.base.MoreObjects.toStringHelper(this.getClass())
                .omitNullValues()
                .add("activityAssociations", getLineItemActivityAssociations().toString())
                .add("actualDeliveryPercentage", this.getActualDeliveryPercentage())
                .add("allowOverbook", getAllowOverbook())
                .add("allowedFormats", this.get_allowedFormats().toString())
                .add("appliedLabels", this.get_appliedLabels())
                .add("autoExtensionDays", getAutoExtensionDays())
                .add("budgetCurrencyCode", this.getBudgetCurrencyCode())
                .add("budgetMicroAmount", this.getBudgetMicroAmount())
                .add("childContentEligibility", this.get_childContentEligibility())
                .add("clicksDelivered", this.getClicksDelivered())
                .add("companionDeliveryOption", this.get_companionDeliveryOption())
                .add("competitiveConstraintScope", this.get_competitiveConstraintScope())
                .add("contractedUnitsBought", getContractedUnitsBought())
                .add("costPerUnitCurrencyCode", this.getCostPerUnitCurrencyCode())
                .add("costPerUnitMicroAmount", this.getCostPerUnitMicroAmount())
                .add("costType", this.get_costType())
                .add("creationDateTime", this.getCreationDateTimeString())
                .add("creativeRotationType", this.get_creativeRotationType())
                // Only include length of creativeTargetings to avoid overly verbose output
                .add("creativeTargetings.length", getCreativeTargetings() == null ? 0 : getCreativeTargetings().length)
                .add("customFieldValueIds", this.getCustomFieldValueIds())
                .add("customPacingGoals", this.getCustomPacingGoals().toString())
                .add("customVastExtension", getCustomVastExtension())
                .add("deliveryDataUnits", this.getDeliveryDataUnits())
                .add("deliveryForecastSource", this.get_deliveryForecastSource())
                .add("deliveryRateType", this.get_deliveryRateType())
                .add("disableSameAdvertiserCompetitiveExclusion", getDisableSameAdvertiserCompetitiveExclusion())
                .add("discount", getDiscount())
                .add("discountType", this.get_discountType())
                .add("effectiveAppliedLabels", this.get_effectiveAppliedLabels().toString())
                .add("endDateTime", this.getEndDateTimeString())
                .add("environmentType", this.get_environmentType())
                .add("expectedDeliveryPercentage", this.getExpectedDeliveryPercentage())
                .add("externalDealId", this.getExternalDealId())
                .add("externalId", getExternalId())
                .add("groupSettings", this.getGroupSettings())
                .add("id", getId())
                .add("impressionsDelivered", this.getImpressionsDelivered())
                .add("isArchived", getIsArchived())
                .add("isMissingCreatives", getIsMissingCreatives())
                .add("lastModifiedByApp", getLastModifiedByApp())
                .add("lastModifiedDateTime", this.getLastModifiedDateTimeString())
                .add("lineItemCreativePlaceholders", this.getLineItemCreativePlaceholders().toString())
                .add("lineItemFrequencyCaps", this.getLineItemFrequencyCaps().toString())
                .add("lineItemType", this.get_lineItemType())
                .add("name", getName())
                .add("notes", getNotes())
                .add("orderId", getOrderId())
                .add("orderName", getOrderName())
                .add("primaryGoal", this.getPrimaryGoal().toString())
                .add("priority", getPriority())
                .add("programmaticCreativeSource", this.get_programmaticCreativeSource())
                .add("reportDate", this.getReportDate())
                .add("reportDateTime", this.getReportCreateTimestamp())
                .add("reservationStatus", this.get_reservationStatus())
                .add("reserveAtCreation", getReserveAtCreation())
                .add("roadblockingType", this.getRoadblockingType())
                .add("secondaryGoals", this.get_secondaryGoals())
                .add("skipCrossSellingRuleWarningChecks", getSkipCrossSellingRuleWarningChecks())
                .add("skipInventoryCheck", getSkipInventoryCheck())
                .add("skippableAdType", this.get_skippableAdType())
                .add("startDateTime", this.getStartDateTimeString())
                .add("startDateTimeType", this.get_startDateTimeType())
                .add("status", this.get_status())
                // Exclude targeting to avoid overly verbose output
                .add("thirdPartyMeasurementSettings", this.getThirdPartyMeasurementSettings().toString())
                .add("unlimitedEndDateTime", getUnlimitedEndDateTime())
                .add("valueCostPerUnitCurrencyCode", this.getValueCostPerUnitCurrencyCode())
                .add("valueCostPerUnitMicroAmount", this.getValueCostPerUnitMicroAmount())
                .add("videoCompletionsDelivered", this.getVideoCompletionsDelivered())
                .add("videoMaxDuration", getVideoMaxDuration())
                .add("videoStartsDelivered", this.getVideoStartsDelivered())
                .add("viewabilityProviderCompanyIds", getViewabilityProviderCompanyIds())
                .add("viewableImpressionsDelivered", this.getViewableImpressionsDelivered())
                .add("webPropertyCode", getWebPropertyCode())
                .toString();
    }

    private static class GamCustomPacingGoal extends CustomPacingGoal {
        @SerializedName("customPacingGoalStartDateTime")
        private String startDateTime;

        protected GamCustomPacingGoal(CustomPacingGoal goal) {
            super();
            this.startDateTime = GamUtils.dateTimeToString(goal.getStartDateTime());
        }
    }

    private void setGamCustomPacingGoals(@Nullable CustomPacingCurve curve) {
        if (curve != null) {
            setGamCustomPacingGoals(curve.getCustomPacingGoals());
        } else {
            log.debug("No Custom Pacing Curve detected");
        }
    }

    private void setGamCustomPacingGoals(@Nullable CustomPacingGoal[] goals) {
        if (goals != null) {
            this.customPacingGoals = Lists.newArrayList();
            for (CustomPacingGoal goal : goals) {
                this.customPacingGoals.add(new GamCustomPacingGoal(goal));
            }
        } else {
            log.debug("No Custom Pacing Goals detected");
        }
    }

    @EqualsAndHashCode(callSuper = true)
    private static class GamFrequencyCap extends FrequencyCap {
        @SerializedName("frequencyCapTimeUnit")
        private String timeUnit;

        protected GamFrequencyCap(FrequencyCap frequencyCap) {
            super();
            this.timeUnit = frequencyCap.getTimeUnit().getValue();
        }

    }

    private void setLineItemFrequencyCaps(@Nullable FrequencyCap[] frequencyCaps) {
        if (frequencyCaps != null) {
            this.lineItemFrequencyCaps = Lists.newArrayList();
            for (FrequencyCap frequencyCap : frequencyCaps) {
                this.lineItemFrequencyCaps.add(new GamFrequencyCap(frequencyCap));
            }
        } else {
            log.debug("No Frequency Caps detected");
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    private static class GamCreativePlaceholder extends CreativePlaceholder {
        @SerializedName("creativePlaceholderWidth")
        private int width;
        @SerializedName("creativePlaceholderHeight")
        private int height;
        @SerializedName("creativePlaceholderIsAspectRatio")
        private Boolean isAspectRatio;

        protected GamCreativePlaceholder(CreativePlaceholder creativePlaceholder) {
            super();
            this.width = creativePlaceholder.getSize().getWidth();
            this.height = creativePlaceholder.getSize().getHeight();
            this.isAspectRatio = creativePlaceholder.getSize().getIsAspectRatio();
        }

    }

    private void setLineItemCreativePlaceholders(@Nullable CreativePlaceholder[] creativePlaceholders) {
        if (creativePlaceholders != null) {
            this.lineItemCreativePlaceholders = Lists.newArrayList();
            for (CreativePlaceholder creativePlaceholder : creativePlaceholders) {
                this.lineItemCreativePlaceholders.add(new GamCreativePlaceholder(creativePlaceholder));
            }
        } else {
            log.debug("No Creative Placeholders detected");
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    private static class GamActivityAssociation extends LineItemActivityAssociation {
        private String clickThroughConversionCostCurrencyCode;
        private long clickThroughConversionCostMicroAmount;
        private String viewThroughConversionCostCurrencyCode;
        private long viewThroughConversionCostMicroAmount;

        protected GamActivityAssociation(LineItemActivityAssociation activityAssociation) {
            super();
            this.clickThroughConversionCostCurrencyCode = activityAssociation.getClickThroughConversionCost().getCurrencyCode();
            this.clickThroughConversionCostMicroAmount = activityAssociation.getClickThroughConversionCost().getMicroAmount();
            this.viewThroughConversionCostCurrencyCode = activityAssociation.getViewThroughConversionCost().getCurrencyCode();
            this.viewThroughConversionCostMicroAmount = activityAssociation.getViewThroughConversionCost().getMicroAmount();
        }

        @Override
        public String toString() {
            return com.google.common.base.MoreObjects.toStringHelper(this.getClass())
                    .omitNullValues()
                    .add("activityId", getActivityId())
                    .add("clickThroughConversionCostCurrencyCode", getClickThroughConversionCostCurrencyCode())
                    .add("clickThroughConversionCostMicroAmount", getClickThroughConversionCostMicroAmount())
                    .add("viewThroughConversionCostCurrencyCode", getViewThroughConversionCostCurrencyCode())
                    .add("viewThroughConversionCostMicroAmount", getViewThroughConversionCostMicroAmount())
                    .toString();
        }
    }

    public String getStartDateTimeString() { return this.startDateTime; }

    public String getEndDateTimeString() { return this.endDateTime; }

    public String getCreationDateTimeString() { return this.creationDateTime; }

    public String getLastModifiedDateTimeString() { return this.lastModifiedDateTime; }

    private void setLineItemActivityAssociations(@Nullable LineItemActivityAssociation[] activityAssociations) {
        if (activityAssociations != null) {
            this.lineItemActivityAssociations = Lists.newArrayList();
            for (LineItemActivityAssociation activityAssociation : activityAssociations) {
                this.lineItemActivityAssociations.add(new GamActivityAssociation(activityAssociation));
            }
        } else {
            log.debug("No Activity Associations detected");
        }
    }

    private void setGamAllowedFormats(@Nullable AllowedFormats[] allowedFormats) {
        if (allowedFormats != null) {
            this._allowedFormats = Lists.newArrayList();
            for (AllowedFormats allowedFormat : allowedFormats) {
                this._allowedFormats.add(allowedFormat.getValue());
            }
        } else {
            log.debug("No Allowed Formats detected");
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    private static class GamAppliedLabel extends AppliedLabel {

        @SerializedName("appliedLabelId")
        private long id;
        @SerializedName("appliedLabelIsNegated")
        private boolean isNegated;

        protected GamAppliedLabel(AppliedLabel appliedLabel) {
            super();
            this.id = appliedLabel.getLabelId();
            this.isNegated = appliedLabel.getIsNegated();
        }
    }

    private List<GamAppliedLabel> convertLabelList(AppliedLabel[] appliedLabels) {
        List<GamAppliedLabel> newList = Lists.newArrayList();
        for (AppliedLabel appliedLabel : appliedLabels) {
            newList.add(new GamAppliedLabel(appliedLabel));
        }
        return newList;
    }

    private List<Long> convertCustomValuesLongList(@Nullable BaseCustomFieldValue[] customFieldValues) {
        if (customFieldValues != null) {
            List<Long> newList = Lists.newArrayList();

            for (BaseCustomFieldValue value : customFieldValues) {
                newList.add(value.getCustomFieldId());
            }
            return newList;
        } else {
            log.debug("No Custom Field Values detected");
            return null;
        }
    }

    private static class GamThirdPartyMeasurementSettings extends ThirdPartyMeasurementSettings {
        @SerializedName("thirdPartyViewabilityPartner")
        private final String viewabilityPartner;
        @SerializedName("thirdPartyPublisherViewabilityPartner")
        private final String publisherViewabilityPartner;
        @SerializedName("thirdPartyBrandLiftPartner")
        private final String brandLiftPartner;
        @SerializedName("thirdPartyReachPartner")
        private final String reachPartner;
        @SerializedName("thirdPartyPublisherReachPartner")
        private final String publisherReachPartner;

        protected GamThirdPartyMeasurementSettings(ThirdPartyMeasurementSettings thirdPartyMeasurementSettings) {
            super();
            this.viewabilityPartner = thirdPartyMeasurementSettings.getViewabilityPartner().getValue();
            this.setViewabilityClientId(thirdPartyMeasurementSettings.getViewabilityClientId());
            this.setViewabilityReportingId(thirdPartyMeasurementSettings.getViewabilityReportingId());
            this.publisherViewabilityPartner = thirdPartyMeasurementSettings.getPublisherViewabilityPartner().getValue();
            this.setPublisherViewabilityClientId(thirdPartyMeasurementSettings.getPublisherViewabilityClientId());
            this.setPublisherViewabilityReportingId(thirdPartyMeasurementSettings.getPublisherViewabilityReportingId());
            this.brandLiftPartner = thirdPartyMeasurementSettings.getBrandLiftPartner().getValue();
            this.setBrandLiftClientId(thirdPartyMeasurementSettings.getBrandLiftClientId());
            this.setBrandLiftReportingId(thirdPartyMeasurementSettings.getBrandLiftReportingId());
            this.reachPartner = thirdPartyMeasurementSettings.getReachPartner().getValue();
            this.setReachClientId(thirdPartyMeasurementSettings.getReachClientId());
            this.setReachReportingId(thirdPartyMeasurementSettings.getReachReportingId());
            this.publisherReachPartner = thirdPartyMeasurementSettings.getPublisherReachPartner().getValue();
            this.setPublisherReachClientId(thirdPartyMeasurementSettings.getPublisherReachClientId());
            this.setPublisherReachReportingId(thirdPartyMeasurementSettings.getPublisherReachReportingId());
        }
    }

    private static class GamGoal extends Goal {
        @SerializedName("lineItemGoalType")
        private final String goalType;
        @SerializedName("lineItemUnitType")
        private final String unitType;
        @SerializedName("lineItemUnits")
        private final long units;

        protected GamGoal(Goal goal) {
            this.goalType = goal.getGoalType().getValue();
            this.unitType = goal.getUnitType().getValue();
            this.units = goal.getUnits();
        }
    }

    private List<GamGoal> convertGoalsToList(Goal[] goals) {
        List<GamGoal> newList = Lists.newArrayList();
        for (Goal goal : goals) {
            newList.add(new GamGoal(goal));
        }
        return newList;
    }

    private static class GamGroupSettings extends GrpSettings {
        @SerializedName("groupSettingsTargetGender")
        private String targetGender;
        @SerializedName("groupSettingsProvider")
        private String provider;
        @SerializedName("groupSettingsNielsenCtvPacingType")
        private String nielsenCtvPacingType;
        @SerializedName("groupSettingsPacingDeviceCategorization")
        private String pacingDeviceCategorization;

        protected GamGroupSettings(GrpSettings grpSettings) {
            this.setMinTargetAge(grpSettings.getMinTargetAge());
            this.setMaxTargetAge(grpSettings.getMaxTargetAge());
            this.targetGender = grpSettings.getTargetGender().getValue();
            this.provider = grpSettings.getProvider().getValue();
            this.setTargetImpressionGoal(grpSettings.getTargetImpressionGoal());
            this.nielsenCtvPacingType = grpSettings.getNielsenCtvPacingType().getValue();
            this.pacingDeviceCategorization = grpSettings.getPacingDeviceCategorizationType().getValue();
        }
    }

    public String toJson() {
        ExclusionStrategy excludeSuperClass = new GamJsonSerializationExclusionStrategy(LineItem.class);
        Gson gson = new GsonBuilder()
                .setExclusionStrategies(excludeSuperClass)
                .create();
        return gson.toJson(this);
    }

}
