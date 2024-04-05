package com.target.kelsaapi.common.vo.tradedesk;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Advertiser {

    @JsonProperty("PartnerId")
    public String partnerId;
    @JsonProperty("AdvertiserId")
    public String advertiserId;
    @JsonProperty("AdvertiserName")
    public String advertiserName;
    @JsonProperty("Description")
    public Object description;
    @JsonProperty("CurrencyCode")
    public String currencyCode;
    @JsonProperty("AttributionClickLookbackWindowInSeconds")
    public int attributionClickLookbackWindowInSeconds;
    @JsonProperty("AttributionImpressionLookbackWindowInSeconds")
    public int attributionImpressionLookbackWindowInSeconds;
    @JsonProperty("ClickDedupWindowInSeconds")
    public int clickDedupWindowInSeconds;
    @JsonProperty("ConversionDedupWindowInSeconds")
    public int conversionDedupWindowInSeconds;
    @JsonProperty("DefaultRightMediaOfferTypeId")
    public int defaultRightMediaOfferTypeId;
    @JsonProperty("IndustryCategoryId")
    public int industryCategoryId;
    @JsonProperty("Keywords")
    public ArrayList<Object> keywords;
    @JsonProperty("Availability")
    public String availability;
    @JsonProperty("LogoURL")
    public Object logoURL;
    @JsonProperty("DomainAddress")
    public String domainAddress;
    @JsonProperty("CustomLabels")
    public ArrayList<Object> customLabels;
    @JsonProperty("AssociatedBidLists")
    public ArrayList<Object> associatedBidLists;
    @JsonProperty("IgnoreReferralUrlInConversion")
    public boolean ignoreReferralUrlInConversion;
    @JsonProperty("IsBallotMeasure")
    public boolean isBallotMeasure;
    @JsonProperty("IsCandidateElection")
    public boolean isCandidateElection;
    @JsonProperty("CandidateProfiles")
    public ArrayList<Object> candidateProfiles;
    @JsonProperty("VettingStatus")
    public String vettingStatus;
    @JsonProperty("Increments")
    public ArrayList<Object> increments;
    @JsonProperty("DataPolicies")
    public ArrayList<Object> dataPolicies;
}