package com.target.kelsaapi.common.vo.google.state.admanager.forecast;

import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;

@Data
public class GamForecastContendingLineItemsId implements Serializable {
    @Id
    private String pipelineRunId;

    @Id
    private String reportStartDate;

    @Id
    private Long availabilityLineItemId;

    public GamForecastContendingLineItemsId(String pipelineRunId, String reportStartDate, Long availabilityLineItemId) {
        this.pipelineRunId = pipelineRunId;
        this.reportStartDate = reportStartDate;
        this.availabilityLineItemId = availabilityLineItemId;
    }

    public GamForecastContendingLineItemsId() {}
}
