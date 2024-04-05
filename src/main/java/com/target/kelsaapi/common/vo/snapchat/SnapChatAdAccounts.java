package com.target.kelsaapi.common.vo.snapchat;

import lombok.Data;

@Data
public class SnapChatAdAccounts {
    private String request_status;
    private String request_id;
    private Paging paging;
    private SnapChatAdAccount[] adaccounts;

    @Data
    public static class SnapChatAdAccount {
        private String sub_request_status;
        private AdAccount adaccount;
    }

    @Data
    public static class AdAccount {
        private String id;
        private String updated_at;
        private String created_at;
        private String name;
        private String type;
        private String status;
        private String organization_id;
        private String[] funding_source_ids;
        private String currency;
        private String timezone;
        private String advertiser_organization_id;
        private String billing_center_id;
        private String billing_type;
        private long lifetime_spend_cap_micro;
        private boolean agency_representing_client;
        private boolean client_paying_invoices;
    }
}