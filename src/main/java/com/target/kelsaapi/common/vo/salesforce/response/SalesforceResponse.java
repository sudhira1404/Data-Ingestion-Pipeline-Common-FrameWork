package com.target.kelsaapi.common.vo.salesforce.response;

import lombok.Data;

import java.util.List;

@Data
public class SalesforceResponse {
    private List records;
    private String totalSize;
    private String nextRecordsUrl;
    private boolean done;
}
