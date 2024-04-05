package com.target.kelsaapi.common.service.google.admanager.forecast;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastContendingLineItemsService;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastLoopStatus;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastStateService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.forecast.GamLineItemForecastRequest;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastContendingLineItemsId;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastStateId;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.util.Pair;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This is a helper service that acts as an intermediary between {@link GamForecastOrchestrationService} and
 * {@link GamForecastAsyncThreadHandler}. It is primarily responsible for queueing all asynchronous worker threads through
 * the {@link ThreadPoolTaskExecutor}. Each async worker thread request placed on the thread pool eventually runs a {@link GamForecastAsyncThreadHandler}.
 * Since it takes much longer for async worker threads to receive responses from the {@link com.google.api.ads.admanager.axis.v202311.ForecastService} API
 * than it does this service to add async worker thread requests to the thread pool, this service also handles back pressure from
 * the thread pool to pause on adding more requests until there are open slots again on the thread pool to queue more
 * async threads.
 */
@Service
@Slf4j
public class GamForecastQueueService {

    private final GamForecastStateService repository;

    private final GamForecastContendingLineItemsService contendingLineItemsRepository;

    private final GamForecastAsyncThreadHandler asyncThreadHandler;

    private final ThreadPoolTaskExecutor threadPool;

    private final int contendingLineItemsSize;

    private final int DEFAULT_INITIAL_INTERVAL;

    private final int DEFAULT_MAX_RETRY_INTERVAL;

    private final int DEFAULT_TOTAL_TIME_TO_WAIT;

    private final ThreadPoolExecutor queue;

    @Autowired
    public GamForecastQueueService(GamForecastStateService repository,
                                   GamForecastAsyncThreadHandler asyncThreadHandler,
                                   PipelineConfig config,
                                   @Qualifier("gamForecastExecutor")
                                   ThreadPoolTaskExecutor threadPoolExecutor,
                                   GamForecastContendingLineItemsService contendingLineItemsRepository) {
        this.repository = repository;
        this.contendingLineItemsRepository = contendingLineItemsRepository;
        this.asyncThreadHandler = asyncThreadHandler;
        this.threadPool = threadPoolExecutor;
        this.queue = this.threadPool.getThreadPoolExecutor();
        PipelineConfig.Google.AdManager.RetryBackoff backoff = config.apiconfig.source.google.adManager.forecast.queueingAsyncThreads.retryBackoff;
        this.DEFAULT_INITIAL_INTERVAL = backoff.initialIntervalSeconds * 1000;
        this.DEFAULT_MAX_RETRY_INTERVAL = backoff.maxRetryIntervalSeconds * 1000;
        this.DEFAULT_TOTAL_TIME_TO_WAIT = backoff.totalTimeToWaitMinutes * 60 * 1000;
        PipelineConfig.Google.AdManager.Forecast forecast = config.apiconfig.source.google.adManager.forecast;
        this.contendingLineItemsSize = forecast.contendingLineItemSize;
    }


    /**
     * Used by {@link GamForecastOrchestrationService} to monitor statuses for all async workers in a given pipeline run.
     * This leverages the Postgres table `gam_forecast_state` to pull statuses to better understand how many async workers
     * are initialized/running/failed/succeeded. It will continue to block the caller and repeat the Postgres checks
     * using a {@link BackOff} strategy to control its sleep-retry cycles.
     *
     * @param pipelineRunId The ID of the pipeline run.
     * @param finalNumber The expected total number of succeeded/failed records in Postgres.
     * @param forecastType The type of Forecast to check, from {@link ApplicationConstants.GamForecastTypes} enum.
     * @throws IOException Thrown by {@link BackOff} object when initializing its nextBackOffMillis() method.
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    protected void getForecasts(String pipelineRunId,
                                int finalNumber,
                                ApplicationConstants.GamForecastTypes forecastType
                                ) throws IOException, InterruptedException {
        MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME,pipelineRunId);
        int attempt = 1;
        BackOff backOff = CommonUtils.startBackOff(DEFAULT_INITIAL_INTERVAL, DEFAULT_MAX_RETRY_INTERVAL, DEFAULT_TOTAL_TIME_TO_WAIT);
        long retryInterval = backOff.nextBackOffMillis();
        boolean retry = true;

        while (retry) {
            if (queue.isTerminating() || queue.isTerminated() || queue.isShutdown()) throw new InterruptedException("This thread is shutting down!");
            Boolean isQueued = repository.isQueued(pipelineRunId,forecastType.name().toLowerCase());
            if (isQueued) {
                GamForecastLoopStatus status = repository.getStatuses(pipelineRunId, forecastType.name().toLowerCase());
                int initializedNumber = status.getInitializedTotal();
                int finishedNumber = status.getCompletedTotal();
                int runningNumber = status.getRunningTotal();
                int failedNumber = status.getFailedTotal();

            if ((initializedNumber + runningNumber) == 0) {
                log.info("All Gam {} forecasts have started running now, checking further statuses.", forecastType.name());
                    if (runningNumber == 0 && (finishedNumber + failedNumber) == finalNumber) {
                        log.info("All Gam {} forecasts have finished, checking for failures.", forecastType.name());
                        if (failedNumber > 0) {
                            log.warn("There were {} failed forecasts in this loop. Check the gam_forecast_state " +
                                            "table in the database " +
                                            "where forecast_type = '{}' and pipeline_run_id='{}' and status='failed' " +
                                            "to view these failures.",
                                    failedNumber, forecastType.name(), pipelineRunId);
                        } else {
                            log.info("All {} of {} Gam {} forecast requests were completed for pipeline {} on loop {}",
                                    finishedNumber, finalNumber, forecastType.name(), pipelineRunId, 1);
                        }
                    } else {
                        log.warn("Numbers of finished ({}) and failed ({}) forecasts do not balance to expected final number ({})!!",
                                finishedNumber, failedNumber, finalNumber);
                    }
                    retry = false;

                } else {
                    log.info("There are still {} of {} pending Gam {} forecast requests pending for pipeline {} on check number {}",
                            (initializedNumber + runningNumber), finalNumber, forecastType.name(), pipelineRunId, attempt);
                }
            } else {
                log.info("Still waiting on queueing workers, will retry the check");
            }
            if (retry) {
                try {
                    Pair<Integer, Long> wait = GamUtils.wait(attempt, retryInterval, backOff);
                    attempt = wait.getFirst();
                    retryInterval = wait.getSecond();
                } catch (GamException e) {
                    retry = false;
                    log.error(e.getMessage());
                }
            }
        }
    }


    /**
     * Used by {@link GamForecastOrchestrationService} to request an async worker for each Line Item ID to be forecasted.
     * For each Line Item ID, tries to submit a request to the {@link ThreadPoolTaskExecutor} for a {@link GamForecastAsyncThreadHandler}.
     * If there is a spot on the thread pool for a new request, it finishes and tries the next Line Item ID in the list.
     * If the thread pool does not have room currently in its queue for the request, this method then blocks using a
     * {@link BackOff} strategy to control its sleep-retry cycles until it succeeds to hand the request off to the thread pool.
     *
     * @param prospectiveLineItems The list of Line Item IDs to request async worker threads against.
     * @param pipelineRunId The ID of the pipeline run.
     * @param request A {@link GamLineItemForecastRequest} request object.
     * @param startDate The start date in yyyy-mm-dd format.
     * @throws IOException Thrown by {@link BackOff} object when initializing its nextBackOffMillis() method.
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    protected void queueForecastRequestWorkers(List<Long> prospectiveLineItems,
                                                                    String pipelineRunId,
                                                                    GamLineItemForecastRequest request,
                                                                    String startDate,
                                                                    ApplicationConstants.GamForecastTypes type) throws IOException, InterruptedException {
        GamForecastStateId savedId;

        if (type.equals(ApplicationConstants.GamForecastTypes.AVAILABILITY)) {
            assert prospectiveLineItems != null;
            for (Long prospectiveLineItem : prospectiveLineItems) {
                if (queue.isTerminating() || queue.isTerminated() || queue.isShutdown()) {
                    throw new InterruptedException("This thread is marked for shutdown!");
                }
                log.debug("Initial save of line item {} to Postgres", prospectiveLineItem);
                savedId = repository.initializeNewLineItem(pipelineRunId, prospectiveLineItem, type, startDate);
                log.debug("Attempting to add line item {} request to async thread pool queue", prospectiveLineItem);
                retryQueueForecastThread(pipelineRunId, prospectiveLineItem, request, startDate, savedId, type, prospectiveLineItems);
                log.debug("Added line item {} request to async thread pool queue", prospectiveLineItem);
            }
        } else {
            int batchId = 1;
            while (!prospectiveLineItems.isEmpty()) {
                if (queue.isTerminating() || queue.isTerminated() || queue.isShutdown()) {
                    throw new InterruptedException("This thread is marked for shutdown!");
                }
                Long idToDriveForecastThisLoop = prospectiveLineItems.get(0);
                log.debug("Building list of contending line items for delivery forecasting Line Item Id {}",idToDriveForecastThisLoop);
                List<Long> allContendingForThisId = contendingLineItemsRepository.getContendingLineItems(new GamForecastContendingLineItemsId(pipelineRunId, startDate, idToDriveForecastThisLoop));
                List<Long> lineItemsToForecastTogether = Lists.newArrayList();
                if (allContendingForThisId != null && allContendingForThisId.size() > 1) {
                    List<Long> intermediateList = Lists.newArrayList();
                    log.debug("Now removing any that might have been removed from a prior batch");
                    for (Long lineItemId : allContendingForThisId) {
                        if (prospectiveLineItems.contains(lineItemId)) intermediateList.add(lineItemId);
                    }
                    lineItemsToForecastTogether.addAll(GamUtils.getContendingLineItems(intermediateList, idToDriveForecastThisLoop, contendingLineItemsSize));
                } else {
                    if (prospectiveLineItems.size() >= contendingLineItemsSize) {
                        log.debug("There are no contending line items for {}, so using a random sampling of {} in batch {}",idToDriveForecastThisLoop,contendingLineItemsSize,batchId);
                        lineItemsToForecastTogether.addAll(prospectiveLineItems.subList(0, contendingLineItemsSize));
                    } else {
                        log.debug("There are no contending line items for {}, and this is the last batch, so putting the remaining line items together in batch {}", idToDriveForecastThisLoop, batchId);
                        lineItemsToForecastTogether.addAll(prospectiveLineItems);
                    }
                }
                log.debug("Final list of line items to forecast in batch {}: {}", batchId, lineItemsToForecastTogether);
                repository.initializeNewLineItems(pipelineRunId, lineItemsToForecastTogether, ApplicationConstants.GamForecastTypes.DELIVERY, startDate);
                contendingLineItemsRepository.updateBatchId(startDate, lineItemsToForecastTogether, batchId);
                retryQueueForecastThread(pipelineRunId, idToDriveForecastThisLoop, request, startDate, null, type, lineItemsToForecastTogether);
                prospectiveLineItems.removeAll(lineItemsToForecastTogether);
                batchId++;
            }
        }
    }

    /**
     * Helper method that wraps the async forecast worker request around the {@link BackOff} strategy to sleep-retry
     * until the request is written to the {@link ThreadPoolTaskExecutor} thread pool.
     *
     * @param pipelineRunId The ID of the pipeline run.
     * @param lineItem The Line Item ID to request an async worker thread against.
     * @param request A {@link GamLineItemForecastRequest} request object.
     * @param startDate The start date in yyyy-mm-dd format.
     * @param stateId The {@link GamForecastStateId} of the PK from the initial save to Postgres table `gam_forecast_state` for the Availability Forecast request.
     * @throws IOException Thrown by {@link BackOff} object when initializing its nextBackOffMillis() method.
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    private void retryQueueForecastThread(String pipelineRunId,
                                          Long lineItem,
                                          GamLineItemForecastRequest request,
                                          String startDate,
                                          @Nullable GamForecastStateId stateId,
                                          ApplicationConstants.GamForecastTypes forecastType,
                                          List<Long> forecastableLineItems
                                          ) throws IOException, InterruptedException {
        int attempt = 1;
        BackOff backOff = CommonUtils.startBackOff(DEFAULT_INITIAL_INTERVAL, DEFAULT_MAX_RETRY_INTERVAL, DEFAULT_TOTAL_TIME_TO_WAIT);
        long retryInterval = backOff.nextBackOffMillis();
        boolean retry = true;
        Pair<Integer,Long> wait = Pair.of(attempt,retryInterval);

        while (retry) {
            if (queue.isTerminating() || queue.isTerminated() || queue.isShutdown()) {
                throw new InterruptedException("This thread is marked for shutdown!");
            }

            try {
                asyncThreadHandler.getAsyncForecasts(lineItem, request, startDate, stateId, pipelineRunId, forecastableLineItems, forecastType);
                retry = false;
            } catch (Exception e) {
                try {
                    wait = GamUtils.wait(attempt, retryInterval, backOff);
                } catch (InterruptedException ie) {
                    retry = false;
                } catch (GamException | IOException ge) {
                    log.error(ge.getMessage());
                }
            }
            attempt = wait.getFirst();
            retryInterval = wait.getSecond();
        }
    }

    private void shutdownThreadPool() {
        log.warn("Attempting to shut down GAM Forecast Async Thread Pool");
        queue.shutdownNow();
    }

    /**
     * Acts as a kind of shutdown hook. When the system is shutting down, this is called to make sure any last minute
     * cleanup is performed. This is primarily going to attempt to force shutting the thread pool and any running asynchronous
     * threads down, as well as interrupt this service.
     */
    @PreDestroy
    private void shutdown() {
        shutdownThreadPool();
        Thread id = Thread.currentThread();
        log.warn("Attempting to shut down this thread {}", id.getName());
        id.interrupt();
    }

}
