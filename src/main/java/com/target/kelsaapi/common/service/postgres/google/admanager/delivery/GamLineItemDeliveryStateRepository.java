package com.target.kelsaapi.common.service.postgres.google.admanager.delivery;

import com.target.kelsaapi.common.vo.google.state.admanager.delivery.GamForecastableLineItems;
import com.target.kelsaapi.common.vo.google.state.admanager.delivery.GamForecastableLineItemsId;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository("gamLineItemDeliveryStateRepository")
public interface GamLineItemDeliveryStateRepository extends CrudRepository<GamForecastableLineItems, GamForecastableLineItemsId> {

    @Query(value = "select gfli.lineItemId from GamForecastableLineItems as gfli where gfli.reportStartDate = :reportStartDate")
    List<Long> getLineItemIdsByReportStartDate(@Param("reportStartDate") String startDate);


    @Modifying
    @Transactional
    @Query(value = "delete from GamForecastableLineItems as gfli where gfli.reportStartDate <= :reportStartDate")
    void deleteAllByReportStartDate(@Param("reportStartDate") String startDate);
}
