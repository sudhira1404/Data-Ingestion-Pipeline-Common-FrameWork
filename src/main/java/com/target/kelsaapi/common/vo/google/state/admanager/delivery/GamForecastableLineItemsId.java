package com.target.kelsaapi.common.vo.google.state.admanager.delivery;

import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;

@Data
public class GamForecastableLineItemsId implements Serializable {

    @Id
    private Long lineItemId;

    @Id
    private String reportStartDate;

    public GamForecastableLineItemsId() {}

    public GamForecastableLineItemsId(Long lineItemId, String reportStartDate) {
        this.lineItemId = lineItemId;
        this.reportStartDate = reportStartDate;
    }

}
