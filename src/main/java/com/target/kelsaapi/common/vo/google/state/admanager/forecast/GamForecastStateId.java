package com.target.kelsaapi.common.vo.google.state.admanager.forecast;

import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;

@Data
public class GamForecastStateId implements Serializable {

    @Id
    private String pipelineRunId;

    @Id
    private String reportStartDate;

    @Id
    private Long lineItemId;

    @Id
    private String forecastType;

    public GamForecastStateId(String pipelineRunId, String reportStartDate, Long lineItemId, String forecastType) {
        this.pipelineRunId = pipelineRunId;
        this.reportStartDate = reportStartDate;
        this.lineItemId = lineItemId;
        this.forecastType = forecastType;
    }

    public GamForecastStateId() {

    }

}
