package com.target.kelsaapi.common.service.postgres.google.admanager.forecast;

import com.google.api.ads.admanager.axis.v202311.AvailabilityForecast;
import com.google.api.ads.admanager.axis.v202311.ContendingLineItem;
import com.google.common.collect.Lists;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastContendingLineItems;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastContendingLineItemsId;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Repository
@Service
@Slf4j
public class GamForecastContendingLineItemsService {
    private final GamForecastContendingLineItemsRepository repository;

    @Autowired
    public GamForecastContendingLineItemsService(GamForecastContendingLineItemsRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void saveContendingLineItems(AvailabilityForecast availabilityForecast, String pipelineRunId, String reportStartDate, List<Long> forecastableLineItems) {
        Long availabilityLineItemId = availabilityForecast.getLineItemId();
        List<Long> allContendingLineItemIds = Lists.newArrayList();
        List<Long> forecastableContendingLineItemIds = Lists.newArrayList(availabilityLineItemId);
        GamForecastContendingLineItemsId saveId = new GamForecastContendingLineItemsId(pipelineRunId, reportStartDate, availabilityLineItemId);
        StopWatch timer = new StopWatch();
        try {
            timer.start();
            ContendingLineItem[] contendingLineItems = availabilityForecast.getContendingLineItems();

            if (contendingLineItems != null) {
                List<ContendingLineItem> contendingLineItemList = Lists.newArrayList(contendingLineItems);
                contendingLineItemList.sort(Comparator.comparing(ContendingLineItem::getContendingImpressions).reversed());

                for (ContendingLineItem contendingLineItem : contendingLineItemList) {
                    allContendingLineItemIds.add(contendingLineItem.getLineItemId());
                }
                log.debug("There are a total of {} contending line items for Line Item ID {} BEFORE removing those which are not forecastable", allContendingLineItemIds.size(), availabilityLineItemId);
                for (Long id : allContendingLineItemIds) {
                    if (forecastableLineItems.contains(id)) {
                        forecastableContendingLineItemIds.add(id);
                    }
                }
                log.debug("There are a total of {} contending line items for Line Item ID {} AFTER removing those which are not forecastable",forecastableContendingLineItemIds.size(), availabilityLineItemId);
            } else {
                log.debug("No contending line items for {}",availabilityLineItemId);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
        } finally {
            repository.saveAndFlush(new GamForecastContendingLineItems(saveId, forecastableContendingLineItemIds));
            timer.stop();
            log.debug("It took {} seconds to save contending line items for Line Item ID {}",timer.getTotalTimeSeconds(), availabilityLineItemId);
        }
    }

    @Transactional
    public void updateBatchId(String startDate, List<Long> availabilityLineIds, long deliveryForecastBatchId) {
        repository.updateDeliveryBatch(startDate, availabilityLineIds, deliveryForecastBatchId);
    }

    public List<Long> getOrderedAvailabilityIds(String pipelineRunId) {
        return repository.getOrderedAvailabilityIds(pipelineRunId);
    }

    public List<Long> getContendingLineItems(GamForecastContendingLineItemsId id) {
        Optional<GamForecastContendingLineItems> record = repository.findById(id);
        if (record.isPresent()) {
            return record.get().getContendingLineItemIds();
        } else {
            return Lists.newArrayList(id.getAvailabilityLineItemId());
        }
    }

    @Transactional
    public void purgePriorDaysData(String startDate) {
        repository.deleteAllByReportStartDate(startDate);
    }

    public int getSavedContendingLineItemCount(String pipelineRunId) {
        return repository.getSavedContendingLineItems(pipelineRunId);
    }
}
