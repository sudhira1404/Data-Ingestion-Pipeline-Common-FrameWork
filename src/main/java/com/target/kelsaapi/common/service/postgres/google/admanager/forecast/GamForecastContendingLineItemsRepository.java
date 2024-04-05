package com.target.kelsaapi.common.service.postgres.google.admanager.forecast;

import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastContendingLineItems;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastContendingLineItemsId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository("gamForecastContendingLineItemsRepository")
public interface GamForecastContendingLineItemsRepository extends JpaRepository<GamForecastContendingLineItems, GamForecastContendingLineItemsId> {

    @Modifying
    @Query(value = "update gam_forecast_contending_line_items c " +
            "set delivery_forecast_batch_id = :batchId, " +
            "delivery_forecast_batch_generate_timestamp = current_timestamp " +
            "where cast(c.report_start_date as date) = cast(:startDate as date) " +
            "and c.availability_line_item_id in (:ids)",
            nativeQuery = true)
    void updateDeliveryBatch(@Param("startDate") String startDate,
                                    @Param("ids") List<Long> availabilityLineIds,
                                    @Param("batchId") Long deliveryForecastBatchId);

    @Query(value = "select c.availability_line_item_id " +
            "from gam_forecast_contending_line_items c " +
            "where cast(c.report_start_date as date) = cast(:startDate as date) " +
            "and c.delivery_forecast_batch_id is null " +
            "order by array_length(c.contending_line_item_ids,1) desc, c.availability_forecast_save_timestamp desc, c.availability_line_item_id",
            nativeQuery = true)
    List<Long> getOrderedAvailabilityIds(@Param("startDate") String startDate);

    @Modifying
    @Transactional
    @Query(value = "delete from gam_forecast_contending_line_items as gfli " +
            "where cast(gfli.report_start_date as date) < cast(:reportStartDate as date)",
        nativeQuery = true)
    void deleteAllByReportStartDate(@Param("reportStartDate") String startDate);

    @Query(value = "select count(distinct availability_line_item_id) as cnt " +
            "from gam_forecast_contending_line_items as gfli " +
            "where pipeline_run_id = :pipelineRunId ",
        nativeQuery = true)
    int getSavedContendingLineItems(@Param("pipelineRunId") String pipelineRunId);
}
