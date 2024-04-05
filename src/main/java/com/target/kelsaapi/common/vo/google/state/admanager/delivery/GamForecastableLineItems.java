package com.target.kelsaapi.common.vo.google.state.admanager.delivery;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name="gam_forecastable_line_items")
@Data
@IdClass(GamForecastableLineItemsId.class)
public class GamForecastableLineItems {

    @Id
    @Column(name = "line_item_id")
    protected Long lineItemId;

    @Id
    @Column(name = "report_start_date")
    protected String reportStartDate;

    public GamForecastableLineItems(GamForecastableLineItemsId id) {
        this.lineItemId = id.getLineItemId();
        this.reportStartDate = id.getReportStartDate();
    }

    public GamForecastableLineItems() {}

}
