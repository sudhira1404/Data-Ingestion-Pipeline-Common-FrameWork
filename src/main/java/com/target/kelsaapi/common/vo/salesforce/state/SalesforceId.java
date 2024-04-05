package com.target.kelsaapi.common.vo.salesforce.state;

import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;

@Data
public class SalesforceId implements Serializable {

    @Id
    private String reportType;


    public SalesforceId() {}

    public SalesforceId(String reportType) {
        this.reportType = reportType;
    }

}
