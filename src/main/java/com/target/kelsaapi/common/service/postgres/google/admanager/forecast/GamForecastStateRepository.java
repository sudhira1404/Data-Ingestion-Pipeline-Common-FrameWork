package com.target.kelsaapi.common.service.postgres.google.admanager.forecast;

import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastState;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastStateId;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Stream;

import static org.hibernate.jpa.AvailableHints.*;

@Repository("gamForecastStateRepository")
public interface GamForecastStateRepository extends JpaRepository<GamForecastState, GamForecastStateId> {

    @Query(value = "SELECT f.pipeline_run_id,f.report_start_date,f.line_item_id,f.forecast_type,f.created_timestamp, " +
            "f.started_timestamp, f.finished_timestamp, f.status, f.response, f.failure_reason, f.total_attempts " +
            "FROM ( " +
            "   select  e.*, row_number() over (partition by e.line_item_id order by e.finished_timestamp desc) as rn " +
            "   from gam_forecast_state e  " +
            "   WHERE cast(e.report_start_date as date) = cast(:startDate as date) " +
            "   AND e.forecast_type = :forecastType " +
            "   AND e.status = :status " +
            ") f WHERE f.rn = 1",
            nativeQuery = true)
    @QueryHints(value = {
            @QueryHint(name = HINT_FETCH_SIZE, value = "500"),
            @QueryHint(name = HINT_CACHEABLE, value = "false"),
            @QueryHint(name = HINT_READ_ONLY, value = "true")
    })
    Stream<GamForecastState> findAllByStartDateAndForecastTypeAndStatus(@Param("startDate") String startDate,
                                                                        @Param("forecastType") String forecastType,
                                                                        @Param("status") String status);

    @Query(value = "select distinct f.line_item_id from gam_forecast_state f " +
            "   WHERE cast(f.report_start_date as date) = cast(:startDate as date) " +
            "   AND f.forecast_type = :forecastType " +
            "   AND f.status = :status " +
            "   ORDER BY f.line_item_id",
            nativeQuery = true)
    List<Long> getAllLineItemIdsByStartDateAndForecastTypeAndStatus(@Param("startDate") String startDate,
                                                                    @Param("forecastType") String forecastType,
                                                                    @Param("status") String status);

    @Query(value = "select sum(a.completed) as completedTotal, " +
            "              sum(a.running) as runningTotal, " +
            "              sum(a.failed) as failedTotal, " +
            "              sum(a.initialized) as initializedTotal from ( " +
            "    select case when lower(s.status) = 'completed' then 1 else 0 end as completed, " +
            "           case when lower(s.status) = 'running' then 1 else 0 end as running, " +
            "           case when lower(s.status) = 'failed' then 1 else 0 end as failed, " +
            "           case when lower(s.status) = 'initialized' then 1 else 0 end as initialized " +
            "    from gam_forecast_state s where s.pipeline_run_id = :pipelineId " +
            "                              and lower(s.forecast_type) = lower(:forecastType)) a",
            nativeQuery = true)
    GamForecastLoopStatus getPipelineAndTypeStatuses(@Param("pipelineId") String pipelineRunId,
                                                     @Param("forecastType") String forecastType);

    @Query(value = "select case when s.cnt>0 then true else false end as isQueued from " +
            "(select count(line_item_id) as cnt from gam_forecast_state " +
            "where pipeline_run_id = :pipelineId " +
            "and forecast_type = :forecastType) s",
            nativeQuery = true)
    Boolean isQueued(@Param("pipelineId") String pipelineRunId,
                     @Param("forecastType") String forecastType);

    @Modifying
    @Transactional
    @Query(value = "delete from gam_forecast_state as gfli " +
            "where cast(gfli.report_start_date as date) < cast(:reportStartDate as date)",
            nativeQuery = true)
    void deleteAllByReportStartDate(@Param("reportStartDate") String startDate);

    @Modifying
    @Transactional
    @Query(value = "update gam_forecast_state " +
            "set status = :status ," +
            "started_timestamp = current_timestamp ," +
            "total_attempts = 1 " +
            "where pipeline_run_id = :pipelineRunId " +
            "and report_start_date = :reportStart " +
            "and line_item_id = :lineItem " +
            "and forecast_type = :type",
        nativeQuery = true)
    void updateStartedState(@Param("pipelineRunId") String pipelineRunId,
                            @Param("reportStart") String reportStartDate,
                            @Param("lineItem") Long lineItemId,
                            @Param("type") String forecastType,
                            @Param("status") String status);

    @Modifying
    @Transactional
    @Query(value = "update gam_forecast_state " +
            "set status = :status ," +
            "started_timestamp = current_timestamp ," +
            "total_attempts = 1 " +
            "where pipeline_run_id = :pipelineRunId " +
            "and report_start_date = :reportStart " +
            "and line_item_id in (:lineItems) " +
            "and forecast_type = :type",
            nativeQuery = true)
    void updateStartedStates(@Param("pipelineRunId") String pipelineRunId,
                            @Param("reportStart") String reportStartDate,
                            @Param("lineItems") List<Long> lineItemIds,
                            @Param("type") String forecastType,
                            @Param("status") String status);

    @Modifying
    @Transactional
    @Query(value = "update gam_forecast_state " +
            "set status = :status ," +
            "finished_timestamp = current_timestamp, " +
            "total_attempts = :attempts, " +
            "failure_reason = :failMsg " +
            "where pipeline_run_id = :pipelineRunId " +
            "and report_start_date = :reportStart " +
            "and line_item_id = :lineItem " +
            "and forecast_type = :type",
            nativeQuery = true)
    void updateFailedState(@Param("pipelineRunId") String pipelineRunId,
                           @Param("reportStart") String reportStartDate,
                           @Param("lineItem") Long lineItemId,
                           @Param("type") String forecastType,
                           @Param("status") String status,
                           @Param("failMsg") String failureReason,
                           @Param("attempts") int attempts);

    @Modifying
    @Transactional
    @Query(value = "update gam_forecast_state " +
            "set status = :status ," +
            "finished_timestamp = current_timestamp, " +
            "total_attempts = :attempts, " +
            "failure_reason = :failMsg " +
            "where pipeline_run_id = :pipelineRunId " +
            "and report_start_date = :reportStart " +
            "and line_item_id in (:lineItems) " +
            "and forecast_type = :type",
            nativeQuery = true)
    void updateFailedStates(@Param("pipelineRunId") String pipelineRunId,
                           @Param("reportStart") String reportStartDate,
                           @Param("lineItems") List<Long> lineItemIds,
                           @Param("type") String forecastType,
                           @Param("status") String status,
                           @Param("failMsg") String failureReason,
                           @Param("attempts") int attempts);

    @Modifying
    @Transactional
    @Query(value = "update gam_forecast_state " +
            "set status = :status ," +
            "finished_timestamp = current_timestamp, " +
            "total_attempts = :attempts, " +
            "response = :response " +
            "where pipeline_run_id = :pipelineRunId " +
            "and report_start_date = :reportStart " +
            "and line_item_id = :lineItem " +
            "and forecast_type = :type",
            nativeQuery = true)
    void updateCompletedState(@Param("pipelineRunId") String pipelineRunId,
                           @Param("reportStart") String reportStartDate,
                           @Param("lineItem") Long lineItemId,
                           @Param("type") String forecastType,
                           @Param("status") String status,
                           @Param("response") String response,
                           @Param("attempts") int attempts);

}
