package com.target.kelsaapi.common.vo.google.state.admanager.forecast;

import com.target.kelsaapi.common.constants.ApplicationConstants;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.springframework.lang.Nullable;

import java.time.Instant;

@Entity
@Table(name="gam_forecast_state")
@Data
@IdClass(GamForecastStateId.class)
public class GamForecastState {
    @Id
    @Column(name = "pipeline_run_id")
    protected String pipelineRunId;

    @Id
    @Column(name = "report_start_date")
    protected String reportStartDate;

    @Id
    @Column(name = "line_item_id")
    protected Long lineItemId;

    @Id
    @Column(name = "forecast_type")
    protected String forecastType;

    @Column(name = "created_timestamp", columnDefinition= "TIMESTAMP WITH TIME ZONE")
    @Nullable
    @CreationTimestamp
    protected Instant createdTimestamp;

    @Column(name = "started_timestamp", columnDefinition= "TIMESTAMP WITH TIME ZONE")
    @Nullable
    protected Instant startedTimestamp;

    @Column(name = "finished_timestamp", columnDefinition= "TIMESTAMP WITH TIME ZONE")
    @UpdateTimestamp
    @Nullable
    protected Instant finishedTimestamp;

    @Column(name = "status")
    protected String status;

    @Column(name = "response")
    @Nullable
    protected String response;

    @Column(name = "failure_reason")
    @Nullable
    protected String failureReason;

    @Column(name = "total_attempts")
    protected int totalAttempts;

    public GamForecastState(GamForecastStateId id) {
        this.pipelineRunId = id.getPipelineRunId();
        this.reportStartDate = id.getReportStartDate();
        this.lineItemId = id.getLineItemId();
        this.forecastType = id.getForecastType();
        this.status = ApplicationConstants.PipelineStates.INITIALIZED.name().toLowerCase();
    }
    public GamForecastState() {}

    public void setStatus(ApplicationConstants.PipelineStates state) {
        this.status = state.name().toLowerCase();
    }

    public GamForecastStateId getId() {
        return new GamForecastStateId(this.pipelineRunId,this.reportStartDate,this.lineItemId,this.forecastType);
    }
}
