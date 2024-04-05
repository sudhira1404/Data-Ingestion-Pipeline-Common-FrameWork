package com.target.kelsaapi.common.vo.salesforce.state;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name="salesforce")
@Data
@IdClass(SalesforceId.class)
public class Salesforce {
    @Id
    @Column(name = "report_type")
    protected String reportType;

    @Column(name = "attributes")
    protected String attributes;

    public Salesforce(SalesforceId id) {
        this.reportType = id.getReportType();
    }

    public Salesforce() {}

}
