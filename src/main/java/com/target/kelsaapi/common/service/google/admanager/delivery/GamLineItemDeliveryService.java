package com.target.kelsaapi.common.service.google.admanager.delivery;

import com.google.api.ads.admanager.axis.utils.v202311.StatementBuilder;
import com.google.api.ads.admanager.axis.v202311.LineItem;
import com.google.api.ads.admanager.axis.v202311.LineItemPage;
import com.google.api.ads.admanager.axis.v202311.LineItemServiceInterface;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.client.auth.oauth2.Credential;
import com.google.errorprone.annotations.DoNotCall;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.file.LocalFileWriterService;
import com.target.kelsaapi.common.service.google.admanager.AdManagerSessionServicesFactoryInterface;
import com.target.kelsaapi.common.service.google.admanager.GamServiceInterface;
import com.target.kelsaapi.common.service.postgres.google.admanager.delivery.GamLineItemDeliveryStateRepository;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import com.target.kelsaapi.common.vo.google.request.admanager.delivery.GamLineItemRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.delivery.GamLineItemResponse;
import com.target.kelsaapi.common.vo.google.state.admanager.delivery.GamForecastableLineItems;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A service layer that interacts with the Google Ad Manager SDK to retrieve Line Item Data
 */
@Service
@Slf4j
public class GamLineItemDeliveryService implements GamServiceInterface {

    private final AdManagerSessionServicesFactoryInterface adManagerServices;

    private final LocalFileWriterService localFileWriterService;

    private final Boolean sampleForecast;

    private final int sampleSize;

    private final GamLineItemDeliveryStateRepository repository;

    private final int pageSize;

    /**
     * The constructor used by Spring Framework to spin up and autowire this singleton service
     *
     * @param adManagerServices The {@link AdManagerSessionServicesFactoryInterface} for initializing Ad Manager Services from
     * @param localFileWriterService The {@link LocalFileWriterService} for initializing the local file writing service from
     */
    @Autowired
    public GamLineItemDeliveryService(AdManagerSessionServicesFactoryInterface adManagerServices,
                              LocalFileWriterService localFileWriterService,
                              PipelineConfig config,
                              GamLineItemDeliveryStateRepository repository) {
        this.adManagerServices = adManagerServices;
        this.localFileWriterService = localFileWriterService;
        this.sampleForecast = config.apiconfig.source.google.adManager.delivery.sample.enabled;
        this.sampleSize = config.apiconfig.source.google.adManager.delivery.sample.size;
        this.pageSize = config.apiconfig.source.google.adManager.delivery.paginationSize;
        this.repository = repository;
    }

    /**
     * @param statementBuilder The {@link com.google.api.ads.admanager.axis.utils.v202311.Pql} formatted {@link StatementBuilder} for
     *                          retrieving an {@link LineItemPage} containing all {@link LineItem} objects matching the filter conditions.
     * @param credential A refreshed Google {@link Credential} object.
     * @param startDate The start date in yyyy-MM-dd format.
     * @param tempFiles A list of {@link Path}. This list will be added to upon successfully writing the line items
     *                  locally and passed back in the corresponding {@link GamLineItemResponse} object.
     * @return A new {@link GamLineItemResponse} object.
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    private GamLineItemResponse getLineItems(StatementBuilder statementBuilder, Credential credential, String startDate,
                                             List<Path> tempFiles, String pipelineRunId) throws GamException {


        try {
            Instant now = Instant.now();
            log.info("Initialize new Ad Manager Session at {}", now);
            AdManagerSession newSession = adManagerServices.initAdManagerSession(credential);

            log.info("Initialize new Line Item Interface");
            LineItemServiceInterface lineItemInterface = adManagerServices.initLineItemService(newSession);

            log.info("Cleanup previous data from Postgres table");
            try {
                repository.deleteAllByReportStartDate(startDate);
            } catch (Exception e) {
                log.error(e.getMessage(), e.getCause());
            }

            log.info("Initialize request for line items...");

            // Set page size for the first page
            statementBuilder.limit(pageSize);

            int totalResultSetSize = 0;
            if (sampleForecast) totalResultSetSize = sampleSize;
            int interval = 1;
            int totalSize = 0;

            do {
                LineItemPage page = lineItemInterface.getLineItemsByStatement(statementBuilder.toStatement());

                if (page.getResults() != null) {
                    if (interval == 1) {
                        totalSize = page.getTotalResultSetSize();
                        log.info("Total number of line items: {}", totalSize);
                    }
                    if (!sampleForecast || totalResultSetSize > totalSize) totalResultSetSize = totalSize;
                    LineItem[] lineItems = page.getResults();
                    log.debug("Total number of line items in page {} of results: {}", interval, lineItems.length);
                    GamLineItemResponse tempResponse = new GamLineItemResponse(lineItems, startDate, pipelineRunId, interval, now);
                    if (GamUtils.reportDateIsToday(now, startDate)) {
                        log.info("Report date is today, so saving line items that are forecastable to Postgres");
                        writeLineItemsToPostgres(tempResponse);
                    } else {
                        log.info("Skipping saving line items to Postgres due to this request falling on a day other than today.");
                    }
                    tempFiles.add(writeDeliveryResponse(tempResponse));
                    interval++;
                }
                statementBuilder.increaseOffsetBy(pageSize);
            } while (statementBuilder.getOffset() < totalResultSetSize);

            log.info("Retrieved a total of {} line items, landing to {} json files locally", totalResultSetSize, interval);

            return new GamLineItemResponse(tempFiles);
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
            throw new GamException(e.getMessage(), e.getCause());
        }
    }

    private Path writeDeliveryResponse(GamLineItemResponse gir) throws GamException, IOException {
        String lineItemFile = CommonUtils.generateTempFileRootPath() + gir.getPipelineRunId() + "_delivery_report-" + gir.getReportDate() + ".json";
        Boolean writeLineItemFile = writeResponses(gir.getResponseList(), lineItemFile);
        if (Boolean.TRUE.equals(writeLineItemFile)) {
            log.info("Successfully landed line items to local file");
            log.debug("Local file name : {}", lineItemFile);
            return Paths.get(lineItemFile);
        } else {
            throw new GamException("All write attempts to local file for Delivery failed");
        }
    }
    /**
     * Uses the {@link GamLineItemResponse#getResponseList()} method and lands them to the specified local file.
     *
     * @param responseList The List of Strings to write to file.
     * @param lineItemFile Name of the local file to land results to.
     * @return True if successful writing to local file. False if it could not write.
     */
    private Boolean writeResponses(List<String> responseList, String lineItemFile) {
        log.info("Successfully retrieved {} line items. Attempting to write to local file now...", responseList.size());
        return localFileWriterService.writeLocalFile(responseList, lineItemFile, false, true);
    }

    private void writeLineItemsToPostgres(GamLineItemResponse lineItems) {
        int batchSize = 50;

        List<LineItem> gli = Arrays.stream(lineItems.getOriginalLineItems()).filter(state ->
                        (!state.getStatus().getValue().equalsIgnoreCase("completed")) &&
                        (state.getLineItemType().getValue().equalsIgnoreCase("standard")) &&
                        (GamUtils.dateTimeToLocalDate(state.getEndDateTime()).isAfter(GamUtils.dateTimeToLocalDate(state.getStartDateTime()))) &&
                        (GamUtils.dateTimeToLocalDate(state.getEndDateTime()).isAfter(LocalDate.now()))
                ).toList();
        int recordCount = gli.size();
        if (recordCount > 0) {
            log.info("Begin writing {} line items to Postgres", recordCount);
            LineItem[] glia = new LineItem[recordCount];
            gli.toArray(glia);
            for (int i = 0; i < recordCount; i = i + batchSize) {
                List<GamForecastableLineItems> li = new GamLineItemResponse(glia, lineItems.getReportDate(), lineItems.getPipelineRunId(), lineItems.getRequestLoopId(), lineItems.getReportDateTime()).getLineItemStates();
                if (i + batchSize > recordCount) {
                    List<GamForecastableLineItems> li1 = li.subList(i, recordCount - 1);
                    repository.saveAll(li1);
                    break;
                }
                List<GamForecastableLineItems> li1 = li.subList(i, i + batchSize);
                repository.saveAll(li1);
            }
            log.info("Completed writing {} line items to Postgres", recordCount);

        } else {
            log.info("No line items in this batch matched the criteria for forecasting, skipping saving to Postgres");
        }

    }

    @Override
    public GamResponse get(GamRequest request, Credential credential) throws GamException {
        GamLineItemRequest lineItemRequest = (GamLineItemRequest) request;
        return getLineItems(lineItemRequest.getLineItemStatementBuilder(), credential, lineItemRequest.getStartDate(),
                lineItemRequest.getTempFileList(), lineItemRequest.getPipelineRunId());
    }

    @Deprecated
    @DoNotCall
    @Override
    public GamResponse get(Credential credential) throws GamException {
        return null;
    }

    @Deprecated
    @DoNotCall
    @Override
    public Credential get() throws GamException {
        return null;
    }
}
