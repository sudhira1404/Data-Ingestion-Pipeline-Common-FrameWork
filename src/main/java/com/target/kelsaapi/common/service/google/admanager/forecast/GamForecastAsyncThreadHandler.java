package com.target.kelsaapi.common.service.google.admanager.forecast;

import com.google.api.ads.admanager.axis.v202311.*;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.google.admanager.AdManagerSessionServicesFactoryInterface;
import com.target.kelsaapi.common.service.google.admanager.GamAuthenticationService;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastContendingLineItemsService;
import com.target.kelsaapi.common.service.postgres.google.admanager.forecast.GamForecastStateService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.forecast.GamLineItemForecastRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.forecast.GamForecastResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.forecast.GamLineItemAvailabilityForecastResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.forecast.GamLineItemDeliveryForecastResponse;
import com.target.kelsaapi.common.vo.google.state.admanager.forecast.GamForecastStateId;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.*;


/**
 * The service used to run each asynchronous thread for a GAM Forecast pipeline run. Each request placed on the
 * {@link org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor} by the {@link GamForecastQueueService} eventually
 * is run by this service in its own background thread. This handles all the low-level interactions with the
 * {@link ForecastService} API to fetch both Availability and Delivery forecasts for a given Line Item ID, and save them
 * both to the Postgres table `gam_forecast_state`.
 *
 */
@Service
@Slf4j
public class GamForecastAsyncThreadHandler {

    private final GamAuthenticationService auth;

    protected final AdManagerSessionServicesFactoryInterface adManagerServices;

    private final GamForecastStateService stateRepository;

    private final GamForecastContendingLineItemsService contendingLineItemsRepository;

    private final int requestTimeoutSeconds;

    private final int DEFAULT_INITIAL_INTERVAL;

    private final int DEFAULT_MAX_RETRY_INTERVAL;

    private final int DEFAULT_TOTAL_TIME_TO_WAIT;

    @Autowired
    public GamForecastAsyncThreadHandler(GamAuthenticationService auth,
                                         AdManagerSessionServicesFactoryInterface adManagerServices,
                                         GamForecastStateService stateRepository,
                                         PipelineConfig config,
                                         GamForecastContendingLineItemsService contendingLineItemsRepository) {
        this.auth = auth;
        this.adManagerServices = adManagerServices;
        this.stateRepository = stateRepository;
        this.contendingLineItemsRepository = contendingLineItemsRepository;
        PipelineConfig.Google.AdManager.Forecast forecast = config.apiconfig.source.google.adManager.forecast;
        this.requestTimeoutSeconds = forecast.asyncThreads.requestTimeoutSeconds;
        this.DEFAULT_INITIAL_INTERVAL = forecast.asyncThreads.retryBackoff.initialIntervalSeconds * 1000;
        this.DEFAULT_MAX_RETRY_INTERVAL = forecast.asyncThreads.retryBackoff.maxRetryIntervalSeconds * 1000;
        this.DEFAULT_TOTAL_TIME_TO_WAIT = forecast.asyncThreads.retryBackoff.totalTimeToWaitMinutes * 60 * 1000;

    }

    /**
     * Entry point for each asynchronous worker thread. This is the method invoked by {@link GamForecastQueueService}
     * for each Line Item ID to be forecasted. This uses the {@link org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor}
     * to first queue, then when a thread is open in the pool, begin running. Once it starts running, it generates a new
     * authentication token and initiates a {@link ForecastServiceInterface}. Using this interface, it requests an Availability
     * Forecast for the Line Item ID, and saves it to the Postgres table `gam_forecast_state`. It then pulls out the Contending Line Items
     * (which is an array of objects inside the response of an Availability Forecast), and uses those to submit a request
     * for the Delivery Forecast for that Line Item and its Contending Line Items. Once it has the Delivery Forecast response,
     * it pulls out only the one forecast for that original Line Item ID and saves that to Postgres.
     * <p>
     * If there is no Availability Forecast for a Line Item, it won't attempt the Delivery Forecast request.
     *
     * @param prospectiveLineItemId This is the Line Item ID to generate both Availability and Delivery Forecasts for.
     * @param request               A {@link GamLineItemForecastRequest} request object.
     * @param startDate             The start date in yyyy-mm-dd format.
     * @param availabilityState     The {@link GamForecastStateId} of the PK from the initial save to Postgres table `gam_forecast_state` for the Availability Forecast request.
     * @param pipelineRunId         The ID of the pipeline run.
     * @throws GamException         Any general application exception.
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    @Async(value = "gamForecastExecutor")
    public void getAsyncForecasts(Long prospectiveLineItemId,
                                  GamLineItemForecastRequest request,
                                  String startDate,
                                  @Nullable GamForecastStateId availabilityState,
                                  String pipelineRunId,
                                  List<Long> forecastableLineItems,
                                  ApplicationConstants.GamForecastTypes forecastType) throws GamException, InterruptedException {
        MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME, pipelineRunId);

        ForecastServiceInterface forecastServiceInterface = initializeService();

        //Availability Forecasting
        if (forecastType.equals(ApplicationConstants.GamForecastTypes.AVAILABILITY)) {
            getAvailabilityForecast(prospectiveLineItemId, request, startDate, availabilityState, forecastServiceInterface,forecastableLineItems, pipelineRunId);
        } else {
            //Delivery Forecasting
            getDeliveryForecast(prospectiveLineItemId, request, startDate, forecastableLineItems, forecastServiceInterface, pipelineRunId);
        }
    }

    /**
     * This helper method generates a new authentication token and initiates a {@link ForecastServiceInterface}.
     *
     * @return An initialized {@link ForecastServiceInterface}
     * @throws GamException Any general application exception.
     */
    private ForecastServiceInterface initializeService() throws GamException, InterruptedException {
        Credential credential;
        AdManagerSession session;
        ForecastServiceInterface forecastServiceInterface;

        try {
            // Get refreshed Oauth token credential
            credential = auth.get();

            log.debug("Initialize new Ad Manager Session");
            session = adManagerServices.initAdManagerSession(credential);

            log.debug("Initialize new Forecast Service Interface");
            forecastServiceInterface = adManagerServices.initForecastServiceInterface(session);

        } catch (GamException | ValidationException e) {
            throw new GamException(e.getMessage(),e.getCause());
        }
        return forecastServiceInterface;
    }

    /**
     * This helper method is a convenience wrapper around the generic retryableForecastRequest() for an Availability Forecast request.
     *
     * @param prospectiveLineItemId This is the Line Item ID to generate both Availability and Delivery Forecasts for.
     * @param request A {@link GamLineItemForecastRequest} request object.
     * @param startDate The start date in yyyy-mm-dd format.
     * @param availabilityState The {@link GamForecastStateId} of the PK from the initial save to Postgres table `gam_forecast_state` for the Availability Forecast request.
     * @param forecastServiceInterface An initialized {@link ForecastServiceInterface}
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    private void getAvailabilityForecast(Long prospectiveLineItemId,
                                        GamLineItemForecastRequest request,
                                        String startDate,
                                        GamForecastStateId availabilityState,
                                        ForecastServiceInterface forecastServiceInterface,
                                        List<Long> forecastableLineItems,
                                        String pipelineRunId) throws InterruptedException, GamException {
        GamLineItemAvailabilityForecastResponse availabilityForecastResponse = null;
        try {
            stateRepository.updateStartedState(availabilityState);
            availabilityForecastResponse = (GamLineItemAvailabilityForecastResponse) retryableForecastRequest(
                    startDate,
                    prospectiveLineItemId,
                    request.getAvailabilityForecastOptions(),
                    null,
                    null,
                    forecastServiceInterface,
                    pipelineRunId
            );
        } catch (IOException | GamException e) {
            log.error(e.getMessage(), e.getCause());
        } catch (InterruptedException e) {
            throw new InterruptedException(e.getMessage());
        } finally {
            if (availabilityForecastResponse != null && (
                    availabilityForecastResponse.getJsonForecast() != null ||
                            availabilityForecastResponse.getFailureReason() != null)) {
                stateRepository.updateForecastFinalState(availabilityForecastResponse, availabilityState);
                if (availabilityForecastResponse.getAvailabilityForecast() != null) {
                    contendingLineItemsRepository.saveContendingLineItems(availabilityForecastResponse.getAvailabilityForecast(),
                            availabilityState.getPipelineRunId(), startDate, forecastableLineItems);
                }
            } else {
                stateRepository.updateForecastFinalState(new GamLineItemAvailabilityForecastResponse(null, availabilityState.getReportStartDate(), DEFAULT_TOTAL_TIME_TO_WAIT, "Failed to retrieve forecast after max time elapsed!"), availabilityState);
            }
        }
    }

    /**
     * This helper method is a convenience wrapper around the generic retryableForecastRequest() for a Delivery Forecast request.
     *
     * @param prospectiveLineItemId This is the Line Item ID to generate both Availability and Delivery Forecasts for.
     * @param request A {@link GamLineItemForecastRequest} request object.
     * @param startDate The start date in yyyy-mm-dd format.
     * @param forecastableLineItems A list of Line Item IDs to be forecasted, presumably returned by an earlier call to getAvailabilityForecast() method.
     * @param forecastServiceInterface An initialized {@link ForecastServiceInterface}
     * @param pipelineRunId The pipeline run ID
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    private void getDeliveryForecast(Long prospectiveLineItemId,
                                     GamLineItemForecastRequest request,
                                     String startDate,
                                     List<Long> forecastableLineItems,
                                     ForecastServiceInterface forecastServiceInterface,
                                     String pipelineRunId
                                     ) throws InterruptedException {
        String deliveryExceptionMessage = null;

        GamLineItemDeliveryForecastResponse deliveryForecastResponse = null;
        try {
            stateRepository.updateStartedStates(forecastableLineItems, pipelineRunId, startDate, ApplicationConstants.GamForecastTypes.DELIVERY.name().toLowerCase());
            deliveryForecastResponse = (GamLineItemDeliveryForecastResponse) retryableForecastRequest(
                    startDate,
                    prospectiveLineItemId,
                    null,
                    forecastableLineItems,
                    request.getDeliveryForecastOptions(),
                    forecastServiceInterface,
                    pipelineRunId
                    );
        } catch (IOException | GamException e) {
            deliveryExceptionMessage = e.getMessage();
            log.error(deliveryExceptionMessage, e.getCause());
        } catch (InterruptedException e) {
            throw new InterruptedException(e.getMessage());
        } finally {
            stateRepository.updateFinalStates(forecastableLineItems, deliveryForecastResponse, deliveryExceptionMessage, pipelineRunId, ApplicationConstants.GamForecastTypes.DELIVERY, startDate);
        }

    }

    /**
     * A generic retryable wrapper method around the low level API calls being made. It is generic from the sense that it
     * supports either an Availability or a Delivery Forecast request.
     * <br></br>
     * <br>For Availability Forecast requests:</br>
     * <pre>{@code
     * retryableForecastRequest(
     *                     startDate,
     *                     prospectiveLineItemId,
     *                     request.getAvailabilityForecastOptions(),
     *                     null,
     *                     null,
     *                     forecastServiceInterface);
     * }</pre>
     * <br></br>
     * <br>For Delivery Forecast requests:</br>
     * <pre>{@code retryableForecastRequest(
     *                         startDate,
     *                         prospectiveLineItemId,
     *                         null,
     *                         GamUtils.getContendingLineItems(availabilityForecastResponse.getAvailabilityForecast(), prospectiveLineItemId, contendingLineItemsSize),
     *                         request.getDeliveryForecastOptions(),
     *                         forecastServiceInterface);
     * }</pre>
     *
     * @param startDate The start date in yyyy-mm-dd format.
     * @param prospectiveLineItemId This is the Line Item ID to generate both Availability and Delivery Forecasts for.
     * @param availabilityForecastOptions These are the {@link AvailabilityForecastOptions} from a {@link GamLineItemForecastRequest}. Provide this only when requesting an Availability Forecast, otherwise pass null.
     * @param prospectiveLineItemIds This is the list of Contending Line Items. Provide this only when requesting a Delivery Forecast, otherwise pass null.
     * @param deliveryForecastOptions These are the {@link DeliveryForecastOptions} from a {@link GamLineItemForecastRequest}. Provide this only when requesting a Delivery Forecast, otherwise pass null.
     * @param forecastServiceInterface An initialized {@link ForecastServiceInterface}
     * @return A {@link GamForecastResponse} which can be cast to either {@link GamLineItemAvailabilityForecastResponse} or {@link GamLineItemDeliveryForecastResponse} depending on what type of request this was for.
     * @throws IOException Thrown by {@link BackOff} object when initializing its nextBackOffMillis() method.
     * @throws InterruptedException If sleep is interrupted by a system-level shutdown event.
     */
    private GamForecastResponse retryableForecastRequest(String startDate,
                                                        Long prospectiveLineItemId,
                                                        @Nullable AvailabilityForecastOptions availabilityForecastOptions,
                                                        @Nullable List<Long> prospectiveLineItemIds,
                                                        @Nullable DeliveryForecastOptions deliveryForecastOptions,
                                                        ForecastServiceInterface forecastServiceInterface,
                                                         String pipelineRunId)
            throws IOException, InterruptedException, GamException {
        int attempt = 1;
        BackOff backOff = CommonUtils.startBackOff(DEFAULT_INITIAL_INTERVAL, DEFAULT_MAX_RETRY_INTERVAL, DEFAULT_TOTAL_TIME_TO_WAIT);
        long retryInterval = backOff.nextBackOffMillis();
        boolean retry=true;
        GamForecastResponse response = null;
        Triple<Boolean,Integer,Long> waitOrDone = Triple.of(retry,attempt,retryInterval);
        java.util.concurrent.TimeUnit timeUnit = TimeUnit.SECONDS;

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        ApplicationConstants.GamForecastTypes forecastType;
        if (availabilityForecastOptions != null) {
            forecastType = ApplicationConstants.GamForecastTypes.AVAILABILITY;
        } else {
            forecastType = ApplicationConstants.GamForecastTypes.DELIVERY;
        }

        long[] currentRequestableLineItems = null;
        while (retry) {
            if (Thread.currentThread().isInterrupted()) {
                executorService.shutdown();
                throw new InterruptedException("This thread is marked for shutdown!");
            }
            log.info("Attempt {} to generate {} forecast for Line Item {}", attempt,forecastType.name().toLowerCase(),prospectiveLineItemId);
            if (availabilityForecastOptions!=null) {
                AvailabilityForecast forecast;
                try {
                    Pair<AvailabilityForecast, String> forecastAttemptResponse = getAvailabilityForecastRequestFuture(
                            forecastServiceInterface, executorService, prospectiveLineItemId, availabilityForecastOptions,
                            pipelineRunId, startDate, Thread.currentThread().getName())
                            .get(requestTimeoutSeconds, timeUnit);
                    if (forecastAttemptResponse.getLeft() != null) {
                        forecast = forecastAttemptResponse.getLeft();
                        response = new GamLineItemAvailabilityForecastResponse(forecast, startDate, attempt, null);
                    } else if (forecastAttemptResponse.getRight() != null) {
                        response = new GamLineItemAvailabilityForecastResponse(null, startDate, attempt, forecastAttemptResponse.getRight());
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedException(e.getMessage());
                } catch (TimeoutException e) {
                    log.error("Timed out while waiting on results!");
                } catch (ExecutionException | CancellationException | GamException e) {
                    log.error(e.getMessage(),e.getCause());
                }

            } else {
                Triple<DeliveryForecast, long[], String> forecast;
                try {
                    if (prospectiveLineItemIds!=null && attempt == 1) {
                        currentRequestableLineItems = new long[prospectiveLineItemIds.size()];
                        int i = 0;
                        for (Long id : prospectiveLineItemIds) {
                            currentRequestableLineItems[i] = id;
                            i++;
                        }
                    } else if (currentRequestableLineItems!=null && currentRequestableLineItems.length > 0 && attempt > 1) {
                        log.debug("Attempt {} with a reduced list of line items to forecast together: {}", attempt, currentRequestableLineItems);
                    } else {
                        throw new GamException("There were no line items passed in the prospectiveLineItemIds array!");
                    }
                    forecast = getDeliveryForecastRequestFuture(forecastServiceInterface, executorService,
                            currentRequestableLineItems,deliveryForecastOptions, pipelineRunId, startDate, Thread.currentThread().getName())
                            .get(requestTimeoutSeconds, timeUnit);
                    if (forecast.getLeft() != null) {
                        response = new GamLineItemDeliveryForecastResponse(forecast.getLeft().getLineItemDeliveryForecasts(), startDate, attempt, currentRequestableLineItems, null);
                    } else if (forecast.getMiddle() != null && forecast.getRight() == null) {
                            currentRequestableLineItems = forecast.getMiddle();
                    } else if (forecast.getRight() != null) {
                        response = new GamLineItemDeliveryForecastResponse(null, startDate, attempt, forecast.getMiddle(), forecast.getRight());
                    } else {
                        throw new GamException("Unrecoverable error trying to delivery forecast: " + Arrays.toString(currentRequestableLineItems));
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedException(e.getMessage());
                } catch (ExecutionException | TimeoutException | CancellationException e) {
                    log.error(e.getMessage(),e.getCause());
                } catch (GamException e) {
                    throw new GamException(e);
                }
            }
            try {
                waitOrDone = GamUtils.waitOrRetryForecastRequest(response, attempt, retryInterval, backOff, forecastType);
            } catch (InterruptedException ie) {
                throw new InterruptedException(ie.getMessage());
            } catch (IOException | GamException e) {
                log.error(e.getMessage(), e.getCause());
            } finally {
                if (response != null) {
                    retry = false;
                } else if (backOff.nextBackOffMillis() == BackOff.STOP) {
                    if (availabilityForecastOptions!=null) {
                        response = new GamLineItemAvailabilityForecastResponse(null, startDate, attempt, "Timed out while waiting on results!");
                    } else {
                        response = new GamLineItemDeliveryForecastResponse(null, startDate, attempt, currentRequestableLineItems, "Timed out while waiting on results!");
                    }
                } else {
                    attempt = waitOrDone.getMiddle();
                    retryInterval = waitOrDone.getRight();
                    retry = waitOrDone.getLeft();
                }
            }
        }
        executorService.shutdown();
        return response;
    }


    /**
     * This method allows for a timeout to be set while waiting on an Availability Forecast request.
     *
     * @param forecastServiceInterface An initialized {@link ForecastServiceInterface}
     * @param executorService An {@link ExecutorService} to run this thread with. This method runs in a separate thread, and so the ExecutorService used here should be initialized as <pre>{@code Executors.newSingleThreadExecutor();}</pre>
     * @param prospectiveLineItemId This is the Line Item ID to generate both Availability and Delivery Forecasts for.
     * @param availabilityForecastOptions These are the {@link AvailabilityForecastOptions} from a {@link GamLineItemForecastRequest}. Provide this only when requesting an Availability Forecast, otherwise pass null.
     * @return An {@link AvailabilityForecast} wrapped inside a {@link Future}. This runs a {@link Callable}, so will return an object and/or throw an exception.
     */
    private Future<Pair<AvailabilityForecast, String>> getAvailabilityForecastRequestFuture(
                                            ForecastServiceInterface forecastServiceInterface,
                                            ExecutorService executorService,
                                            Long prospectiveLineItemId,
                                            AvailabilityForecastOptions availabilityForecastOptions,
                                            String pipelineRunId,
                                            String startDate,
                                            String parentThreadName) throws InterruptedException {
        Callable<Pair<AvailabilityForecast, String>> forecastCallable = () -> {
            MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME,pipelineRunId);
            Thread.currentThread().setName(parentThreadName + "-1");
            log.info("Attempting availability forecast for {}",prospectiveLineItemId);
            AvailabilityForecast forecast = null;
            String errorMessage = null;
            try {
                StopWatch timer = new StopWatch();
                timer.start();
                forecast = forecastServiceInterface.getAvailabilityForecastById(prospectiveLineItemId, availabilityForecastOptions);
                timer.stop();
                log.debug("Seconds spent waiting on forecasting: {}",timer.getTotalTimeSeconds());
            } catch (ApiException ae) {
                Pair<long[],String> exceptionResponse = apiExceptionHandler(ae, null,null, pipelineRunId, prospectiveLineItemId, startDate);
                errorMessage = exceptionResponse.getRight();
            }
            return Pair.of(forecast,errorMessage);
        };

        return executorService.submit(forecastCallable);
    }

    /**
     * This method allows for a timeout to be set while waiting on a Delivery Forecast request.
     *
     * @param forecastServiceInterface An initialized {@link ForecastServiceInterface}
     * @param executorService An {@link ExecutorService} to run this thread with. This method runs in a separate thread, and so the ExecutorService used here should be initialized as <pre>{@code Executors.newSingleThreadExecutor();}</pre>
     * @param prospectiveLineItemIds This is the Line Item ID to generate both Availability and Delivery Forecasts for.
     * @param deliveryForecastOptions These are the {@link DeliveryForecastOptions} from a {@link GamLineItemForecastRequest}. Provide this only when requesting a Delivery Forecast, otherwise pass null.
     * @return A {@link Pair} wrapped inside a {@link Future}. The Pair consists of two objects:
     * <ul>
     *     <li>The left element is a {@link DeliveryForecast}, which is only returned on a successful response. This will be null when the Delivery Forecast is not possible.</li>
     *     <li>The right element is an array of Line Item IDs. On the first attempt made, this is the same list passed in the {@param prospectiveLineItemIds}.
     *     However, if an {@link ApiError} is raised that indicates one or more of the Line Items in the original array cannot be forecasted, this list removes those
     *     Line Items from the returned array.</li>
     * </ul>
     *
     */
    private Future<Triple<DeliveryForecast, long[], String>> getDeliveryForecastRequestFuture(
                                        ForecastServiceInterface forecastServiceInterface,
                                        ExecutorService executorService,
                                        long[] prospectiveLineItemIds,
                                        DeliveryForecastOptions deliveryForecastOptions,
                                        String pipelineRunId,
                                        String startDate,
                                        String parentThreadName) {

        Callable<Triple<DeliveryForecast,long[],String>> forecastCallable = () -> {
            MDC.put(ApplicationConstants.PIPELINE_LOGGER_NAME,pipelineRunId);
            Thread.currentThread().setName(parentThreadName + "-1");
            log.info("Attempting delivery forecast for {}",prospectiveLineItemIds);
            long[] apiExceptionHandlerResponse;
            String apiExceptionHandlerError = null;
            long[] returnableLineItems = prospectiveLineItemIds;
            DeliveryForecast forecast = null;
            if (returnableLineItems.length > 0) {
                try {
                    StopWatch timer = new StopWatch();
                    timer.start();
                    forecast = forecastServiceInterface.getDeliveryForecastByIds(returnableLineItems, deliveryForecastOptions);
                    timer.stop();
                    log.debug("Seconds spent waiting on forecasting: {}",timer.getTotalTimeSeconds());
                } catch (ApiException ae) {
                    Pair<long[], String> exceptionResponse = apiExceptionHandler(ae, deliveryForecastOptions, prospectiveLineItemIds, pipelineRunId, null,startDate);
                    apiExceptionHandlerResponse = exceptionResponse.getLeft();
                    if (apiExceptionHandlerResponse != null && exceptionResponse.getRight() == null) {
                        returnableLineItems = apiExceptionHandlerResponse;
                    } else {
                        apiExceptionHandlerError = exceptionResponse.getRight();
                    }
                } catch (Exception e) {
                    throw new GamException(e.getMessage(), e.getCause());
                }

            } else {
                log.error("Invalid request, empty array of line items are not forecastable!");
            }
            return Triple.of(forecast, returnableLineItems, apiExceptionHandlerError);
        };

        return executorService.submit(forecastCallable);
    }

    /**
     * This convenience helper method takes a {@link ApiException} and determines whether a retry should be made.
     * In certain situations it may decide instead of a retry that the exception error message should be returned instead.
     * If the exception error message is returned, it indicates a retry should not be made. In reality the response here is null
     * for Availability Forecast exceptions. If an Availability Forecast attempt is made that results in an un-retryable condition,
     * a {@link GamException} is thrown. In Delivery Forecasting, one of two elements in the response will have a valid value:
     * <ol>
     *     <li>An array of Line Item Ids is returned when the trigger for the error indicates one of the Line Item Ids in the array passed in the {@param prospectiveLineItemIds} parameter
     *     cannot be forecasted. That Line Item Id gets removed from the original array and passed back to the caller as a new array.</li>
     *     <li>When the first element of the array of Line Item Ids passed in the {@param prospectiveLineItemIds} parameter is not forecastable, then
     *     the error message from the Api exception is returned.</li>
     * </ol>
     *
     * @param ae The {@link ApiException} which was thrown from the current forecast attempt.
     * @param deliveryForecastOptions These are the {@link DeliveryForecastOptions} from a {@link GamLineItemForecastRequest}. Provide this only when requesting a Delivery Forecast, otherwise pass null.
     * @param prospectiveLineItemIds This is the list of Contending Line Items used for the current Delivery Forecast request. Provide this only when requesting a Delivery Forecast, otherwise pass null.
     * @param pipeLineRunId The pipeline run ID of this request
     * @param availabilityLineItemId
     * @return A {@link Pair} consisting of two possible elements:
     * <ul>
     *     <li>The left element is an array of Line Item IDs. On the first attempt made, this is the same list passed in the {@param prospectiveLineItemIds}.
     *      However, if an {@link ApiError} is raised that indicates one or more of the Line Items in the original array cannot be forecasted, this list removes those
     *      Line Items until a Delivery Forecast is retrieved for the first ID in the array.</li>
     *      <li>The right element is an error message associated with the {@link ApiError}. This is usually null, but when there is a value, indicates
     *      that no further attempts should be made, and that error should be written to the Postgres table `gam_forecast_state` instead of a valid forecast.</li>
     * </ul>
     * @throws GamException When the original exception thrown indicates the request cannot be retried for Availability Forecasts only.
     */
    private Pair<long[],String> apiExceptionHandler(ApiException ae, @Nullable DeliveryForecastOptions deliveryForecastOptions,
                                                    @Nullable long[] prospectiveLineItemIds, String pipeLineRunId,
                                                    @Nullable Long availabilityLineItemId, String startDate) throws GamException {
        ApiError[] errors = ae.getErrors();
        String returnableErrorMessage = null;

        for (ApiError error: errors) {
            String errorMessage = error.getErrorString();
            String trigger = error.getTrigger();

            if (apiErrorHandler(error)) {
                if (deliveryForecastOptions != null && prospectiveLineItemIds != null && trigger != null) {
                    long lineItemId;
                    long indexOrLineItemId = Long.parseLong(trigger);
                    if (indexOrLineItemId<=prospectiveLineItemIds.length) {
                        lineItemId = prospectiveLineItemIds[(int) indexOrLineItemId];
                    } else {
                        lineItemId = indexOrLineItemId;
                    }
                    log.warn("Line Item ID {} failed due to {} and will be removed from the array of contending line " +
                            "items for the next Delivery Forecast attempt.", lineItemId, errorMessage);

                    if (prospectiveLineItemIds.length > 1) {
                        stateRepository.updateFinalStates(List.of(lineItemId), null, errorMessage, pipeLineRunId, ApplicationConstants.GamForecastTypes.DELIVERY, startDate);
                        prospectiveLineItemIds = ArrayUtils.removeElement(prospectiveLineItemIds, lineItemId);
                        log.debug("New group of line items to request together: {}", prospectiveLineItemIds);
                    } else {
                        log.warn("Line Item ID {} was the last element in the array!",lineItemId);
                        returnableErrorMessage = errorMessage;
                    }
                } else {
                    if (availabilityLineItemId != null) {
                        log.warn("Line Item ID {} failed due to {} during Availability Forecasting", availabilityLineItemId, errorMessage);
                        returnableErrorMessage = errorMessage;
                    } else {
                        log.error(errorMessage);
                    }
                }
            }
        }
        return Pair.of(prospectiveLineItemIds,returnableErrorMessage);
    }

    /**
     * This is a helper method for the {@code apiExceptionHandler()} method. Used to determine whether the error message
     * coming from the {@link ApiException} should be processed in a way that either halts the retry loops or alters the
     * original request's Contending Line Item array.
     *
     * @param error This is the {@link ApiError} from the array of errors inside the {@link ApiException} to be evaluated.
     * @return True if this error needs further evaluation by the {@code apiExceptionHandler()} method, False if it can be ignored (and a new attempt made).
     */
    private boolean apiErrorHandler(ApiError error) {
        boolean saveErrorAndExit;
        if (error instanceof ForecastError) {
            ForecastErrorReason forecastErrorReason = ((ForecastError) error).getReason();

            saveErrorAndExit = !forecastErrorReason.equals(ForecastErrorReason.EXCEEDED_QUOTA) &&
                    !forecastErrorReason.equals(ForecastErrorReason.INTERNAL_ERROR) &&
                    !forecastErrorReason.equals(ForecastErrorReason.SERVER_NOT_AVAILABLE);
        } else saveErrorAndExit = !(error instanceof ServerError) && !(error instanceof QuotaError) && !(error instanceof InternalApiError);
        return saveErrorAndExit;
    }

    /**
     * Acts as a kind of shutdown hook. When the system is shutting down, this is called to make sure any last minute
     * cleanup is performed. This is primarily going to attempt to force shutting the running asynchronous
     * threads down, as well as interrupt this service.
     */
    @PreDestroy
    private void shutdownThread() {
        Thread id = Thread.currentThread();
        log.warn("Attempting to shut down thread {}", id.getName());
        id.interrupt();
    }

}
