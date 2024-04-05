package com.target.kelsaapi.common.vo.snapchat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Objects;

@Data
@Slf4j
public class SnapChatCampaignResponse {

    private ArrayList<String> campaigns;

    public SnapChatCampaignResponse(String response, String adAccountId, String adAccountName) {
        ArrayList<Campaign> campaignResponses = responseToSnapChatCampaignList(response);
        this.campaigns = decorateCampaigns(campaignResponses, adAccountId, adAccountName);
        log.debug("Total number of campaigns fetched from this request: {}", this.campaigns.size());
    }

    private ArrayList<Campaign> responseToSnapChatCampaignList(String response) {
        Gson gson = new GsonBuilder().create();
        CampaignRequest request = gson.fromJson(response, CampaignRequest.class);
        ArrayList<Campaign> returnList = new ArrayList<>();
        for (CampaignParent campaign : request.getCampaigns()) {
            returnList.add(campaign.getCampaign());
        }
        return returnList;
    }

    private ArrayList<String> decorateCampaigns(ArrayList<Campaign> campaigns, String adAccountId, String adAccountName) {
        ArrayList<String> returnableCampaignList = new ArrayList<>();
        campaigns.forEach(campaign -> {
            DecoratedCampaign decoratedCampaign = new DecoratedCampaign(campaign, adAccountId, adAccountName);
            Gson gson = new Gson();
            returnableCampaignList.add(gson.toJson(decoratedCampaign));
        });
        return returnableCampaignList;
    }

    @Data
    public class DecoratedCampaign {
        private String id;
        private String updated_at;
        private String created_at;
        private String name;
        private String ad_account_id;
        private String ad_account_name;
        private long daily_budget_micro; //nullable in source
        private String status;
        private String objective;
        private String start_time;
        private String end_time;
        private long lifetime_spend_cap_micro;
        private String[] delivery_status;
        private String buy_model;
        private Boolean restricted_delivery_signals;

        public DecoratedCampaign(Campaign campaign, String adAccountId, String adAccountName) {
            this.id = campaign.getId();
            this.updated_at = campaign.getUpdated_at();
            this.created_at = campaign.getCreated_at();
            this.name = campaign.getName();
            this.ad_account_id = adAccountId;
            this.ad_account_name = adAccountName;
            this.daily_budget_micro = Objects.requireNonNullElse(campaign.getDaily_budget_micro(),0L);
            this.status = campaign.getStatus();
            this.objective = campaign.getObjective();
            this.start_time = campaign.getStart_time();
            this.end_time = campaign.getEnd_time();
            this.lifetime_spend_cap_micro = campaign.getLifetime_spend_cap_micro();
            this.delivery_status = campaign.getDelivery_status();
            this.buy_model = campaign.getBuy_model();
            setRestricted_delivery_signals(campaign);
        }

        public void setRestricted_delivery_signals(Campaign campaign) {
            if (campaign.getRegulations() != null) {
                Regulation reg = campaign.getRegulations();
                if (reg.getRestricted_delivery_signals() != null)
                    this.restricted_delivery_signals = reg.getRestricted_delivery_signals();
            }
        }
    }


    @Data
    public static class CampaignRequest {
        private String request_status;
        private String request_id;
        private Paging paging;
        private CampaignParent[] campaigns;
    }

    @Data
    public static class CampaignParent {
        private String sub_request_status;
        private Campaign campaign;
    }

    @Data
    public static class Campaign {
        private String id;
        private String updated_at;
        private String created_at;
        private String name;
        private String ad_account_id;
        private long daily_budget_micro;
        private String status;
        private String objective;
        private String start_time;
        private String end_time;
        private long lifetime_spend_cap_micro;
        private String[] delivery_status;
        private Regulation regulations;
        private String buy_model;
        private MeasurementSpec measurement_spec;
    }

    @Data
    public static class Regulation {
        private Boolean restricted_delivery_signals;
        private String candidate_ballot_information;
    }

    @Data
    public static class MeasurementSpec {
        private String ios_app_id;
        private String android_app_url;
    }

}