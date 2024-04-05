package com.target.kelsaapi.common.vo.google.state.admanager.forecast;

import io.hypersistence.utils.hibernate.type.array.ListArrayType;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.UpdateTimestamp;
import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.List;

@Entity
@Table(name = "gam_forecast_contending_line_items")
@Data
@IdClass(GamForecastContendingLineItemsId.class)
public class GamForecastContendingLineItems {
    @Id
    @Column(name = "pipeline_run_id")
    private String pipelineRunId;

    @Id
    @Column(name = "report_start_date")
    protected String reportStartDate;

    @Id
    @Column(name = "availability_line_item_id")
    private Long availabilityLineItemId;

    @Column(name = "availability_forecast_save_timestamp")
    @CreationTimestamp
    private Instant availabilityForecastSaveTimestamp;

    @Type(ListArrayType.class)
    @Column(
            name = "contending_line_item_ids",
            columnDefinition = "bigint[]"
    )
    private List<Long> contendingLineItemIds;

    @Column(name = "delivery_forecast_batch_id")
    @Nullable
    private Long deliveryForecastBatchId;

    @Column(name = "delivery_forecast_batch_generate_timestamp")
    @Nullable
    @UpdateTimestamp
    private Instant deliveryForecastBatchGenerateTimestamp;

    public GamForecastContendingLineItems(GamForecastContendingLineItemsId id, List<Long> contendingLineItemIds) {
        this.pipelineRunId = id.getPipelineRunId();
        this.reportStartDate = id.getReportStartDate();
        this.availabilityLineItemId = id.getAvailabilityLineItemId();
        setContendingLineItemIds(contendingLineItemIds);
    }

    public GamForecastContendingLineItems() {}

    public GamForecastContendingLineItemsId getId() {
        return new GamForecastContendingLineItemsId(this.pipelineRunId,this.reportStartDate,this.availabilityLineItemId);
    }
}
