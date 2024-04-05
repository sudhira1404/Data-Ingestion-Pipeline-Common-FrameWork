package com.target.kelsaapi.common.service.google.admanager.forecast;

import com.google.common.collect.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.postgres.google.admanager.delivery.GamLineItemDeliveryStateRepository;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastContendingLineItemsService;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastStateService;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.forecast.GamLineItemForecastRequest;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import com.target.kelsaapi.pipelines.google.admanager.GamForecastConsumer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The main entry point for a GAM Forecast pipeline request. Provides the top-level control and public methods
 * for {@link GamForecastConsumer} to interact with.
 */
@Service
@Slf4j
public class GamForecastOrchestrationService {

    private final Boolean sampleForecast;

    private final int sampleSize;

    private final GamForecastQueueService queueService;

    private final GamLineItemDeliveryStateRepository deliveryStateRepository;

    private final GamForecastStateService forecastStateRepository;

    private final GamForecastContendingLineItemsService contendingLineItemsRepository;

    @Autowired
    public GamForecastOrchestrationService(PipelineConfig config,
                                           GamForecastQueueService queueService,
                                           GamLineItemDeliveryStateRepository deliveryStateRepository,
                                           GamForecastStateService forecastStateRepository,
                                           GamForecastContendingLineItemsService contendingLineItemsRepository ) {
        this.sampleForecast = config.apiconfig.source.google.adManager.forecast.sample.enabled;
        this.sampleSize = config.apiconfig.source.google.adManager.forecast.sample.size;
        this.queueService = queueService;
        this.deliveryStateRepository = deliveryStateRepository;
        this.forecastStateRepository = forecastStateRepository;
        this.contendingLineItemsRepository = contendingLineItemsRepository;
    }


    /**
     * Invoked by a {@link GamForecastConsumer} to start a GAM Forecast pipeline run.
     * Upon starting, it gets all the Line Items that are forecastable for the day from Postgres table `gam_forecastable_line_items`.
     * The GAM Delivery pipeline is responsible for writing to that table, and must be run for the current day prior
     * to starting a GAM Forecast pipeline run.
     * <p>
     * Then, using the list of line items to forecast, it starts and waits for the {@link GamForecastQueueService} which
     * handles spinning up a {@link GamForecastAsyncThreadHandler} request for each Line Item through a {@link org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor}.
     * <p>
     * Once all asynchronous GAM Forecast requests for each Line Item have either succeeded or failed, this pulls out
     * all valid forecast json strings from the Postgres table `gam_forecast_state`, writes them to two files (one for
     * Availability Forecasts and the other for Delivery Forecasts). The two files are passed back to the caller in a list.
     *
     * @param request    An initialized {@link GamLineItemForecastRequest} object.
     * @param pipelineRunId The ID of the pipeline run.
     * @return The list of {@link Path} of the two generated json files.
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    public List<Path> getForecasts(GamLineItemForecastRequest request, String pipelineRunId) throws GamException {
        MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME,pipelineRunId);

        try {
            log.info("Cleanup previous data from Postgres gam_forecast_state table");
            forecastStateRepository.purgePriorDaysData(request.getStartDate());
            log.info("Cleanup previous data from Postgres gam_forecast_contending_line_items table");
            contendingLineItemsRepository.purgePriorDaysData(request.getStartDate());
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
        }
        List<Path> fileList = Lists.newArrayList();
        getAllForecasts(request, pipelineRunId, ApplicationConstants.GamForecastTypes.AVAILABILITY);

        log.info("All Availability Forecasts are completed, moving on to Delivery Forecasts");
        getAllForecasts(request, pipelineRunId, ApplicationConstants.GamForecastTypes.DELIVERY);

        log.info("All Delivery Forecasts are completed.");
        try {
            fileList.add(forecastStateRepository.writeAllForecasts(pipelineRunId, ApplicationConstants.GamForecastTypes.AVAILABILITY, ApplicationConstants.PipelineStates.COMPLETED, request.getStartDate()));
        } catch (IOException e) {
            throw new GamException(e.getMessage(),e.getCause());
        }
        try {
            fileList.add(forecastStateRepository.writeAllForecasts(pipelineRunId, ApplicationConstants.GamForecastTypes.DELIVERY, ApplicationConstants.PipelineStates.COMPLETED, request.getStartDate()));
        } catch (IOException e) {
            throw new GamException(e.getMessage(),e.getCause());
        }
        return fileList;
    }

    /**
     * This helper method gets all the Line Item IDs from the Postgres table `gam_forecastable_line_items` matching the date in the request.
     * If the config for sampling forecast is enabled (config.apiconfig.source.google.adManager.forecast.sample.enabled)
     * it reduces the list down on a random sampling of Line Items based on the number configured for sampling
     * (config.apiconfig.source.google.adManager.forecast.sample.size).
     *
     * @param request An instance of {@link GamLineItemForecastRequest}
     * @return List of Line Item IDs to forecast against.
     */
    private List<Long> getProspectiveLineItemIds(GamLineItemForecastRequest request) throws GamException {
        List<Long> lineItemIds = Collections.synchronizedList(deliveryStateRepository.getLineItemIdsByReportStartDate(request.getStartDate()));
        log.info("There are a total of {} line items to forecast today. Checking if any have already been forecasted...",lineItemIds.size());
        List<Long> lineItemsAlreadyAvailabilityForecasted = forecastStateRepository.getAllLineItemIdsByStartDate(request.getStartDate(), ApplicationConstants.GamForecastTypes.AVAILABILITY);
        if (lineItemsAlreadyAvailabilityForecasted != null && !lineItemsAlreadyAvailabilityForecasted.isEmpty()) {
            log.info("There are {} availability forecasts previously completed, will remove them from the working list of forecasts in this pipeline.", lineItemsAlreadyAvailabilityForecasted.size());
            lineItemIds.removeAll(lineItemsAlreadyAvailabilityForecasted);
            log.info("Reduced size of line items to forecast: {}",lineItemIds.size());
        } else {
            log.info("No prior forecasting results from today are in Postgres. Will attempt to forecast all line items.");
        }
        List<Long> lineItemsToForecast = Lists.newArrayList();
        if (sampleForecast) {
            log.info("Only forecasting {} sample size of Line Items due to running in a non-production environment.", sampleSize);
            lineItemsToForecast.addAll(GamUtils.pickRandomProspectiveLineItems(lineItemIds, sampleSize, ThreadLocalRandom.current()));
        } else {
            log.info("Forecasting all line items in the initial request");
            lineItemsToForecast = lineItemIds;
        }
        if (lineItemsToForecast.size() > 0) {
            log.info("Final number of line items to attempt to forecast: {}", lineItemsToForecast.size());
            return lineItemsToForecast;
        } else {
            throw new GamException("There are 0 line items from Postgres to forecast! Make sure that GAM Delivery has run for today's date!");
        }
    }

    private void getAllForecasts(GamLineItemForecastRequest request, String pipelineRunId, ApplicationConstants.GamForecastTypes forecastType)
            throws GamException {
        try {
            List<Long> lineItemsToForecast;
            if (forecastType.equals(ApplicationConstants.GamForecastTypes.AVAILABILITY)) {

                lineItemsToForecast = getProspectiveLineItemIds(request);
                log.info("There are a total of {} Line Items to forecast", lineItemsToForecast.size());
                log.info("Starting to queue Asynchronous Availability Forecast Workers now");
                queueService.queueForecastRequestWorkers(
                        lineItemsToForecast.stream().distinct().toList(),
                        pipelineRunId,
                        request,
                        request.getStartDate(),
                        forecastType
                );

                log.info("Completed queueing all Asynchronous Availability Forecast Workers.");
                log.info("Waiting on all Asynchronous Availability Forecast Workers to complete....");
                queueService.getForecasts(pipelineRunId, lineItemsToForecast.size(), forecastType);

            } else {
                List<Long> deliveryForecastIds = contendingLineItemsRepository.getOrderedAvailabilityIds(request.getStartDate());
                int deliveryForecastIdsSize = deliveryForecastIds.size();
                log.info("There are a total of {} Line Items to forecast", deliveryForecastIdsSize);
                log.info("Starting to queue Asynchronous Delivery Forecast Workers now");
                queueService.queueForecastRequestWorkers(
                        deliveryForecastIds,
                        pipelineRunId,
                        request,
                        request.getStartDate(),
                        forecastType);
                log.info("Completed queueing all Asynchronous Delivery Forecast Workers.");
                log.info("Waiting on all Asynchronous Delivery Forecast Workers to complete....");
                queueService.getForecasts(pipelineRunId, deliveryForecastIdsSize, forecastType);

            }
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
            throw new GamException(e.getMessage(), e.getCause());
        }
    }
}
