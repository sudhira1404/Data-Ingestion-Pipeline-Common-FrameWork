package com.target.kelsaapi.pipelines.google.admanager;

import com.google.api.client.auth.oauth2.Credential;
import com.google.common.collect.Lists;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.service.google.admanager.GamAuthenticationService;
import com.target.kelsaapi.common.service.google.admanager.delivery.GamLineItemDeliveryService;
import com.target.kelsaapi.common.service.google.admanager.delivery.GamOrderService;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.delivery.GamLineItemRequest;
import com.target.kelsaapi.common.vo.google.request.admanager.delivery.GamOrderRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.delivery.GamLineItemResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.delivery.GamOrderResponse;
import com.target.kelsaapi.pipelines.EndPointConsumer;
import com.target.kelsaapi.pipelines.EndPointConsumerInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class GamDeliveryConsumer extends EndPointConsumer implements EndPointConsumerInterface {

    private final GamOrderService orderService;

    private final GamLineItemDeliveryService lineItemService;

    private final GamAuthenticationService auth;


    public GamDeliveryConsumer(ApplicationContext context, String pipelineRunId) {
        super(context, pipelineRunId);

        this.orderService = context.getBean(GamOrderService.class);

        this.lineItemService = context.getBean(GamLineItemDeliveryService.class);

        this.auth = context.getBean(GamAuthenticationService.class);
    }

    @Override
    public void execute(String startDate, String endDate, String targetFile, String reportType, StopWatch stopWatch)
            throws RuntimeException {
        runPipeline(startDate, endDate, targetFile, reportType, stopWatch);
    }

    private void runPipeline(String startDate, String endDate, String targetFile, @Nullable String reportType,
                             StopWatch stopWatch) throws RuntimeException {
        CommonUtils.timerSplit(stopWatch, "Initialization");
        try {
            // Initialize file handling controls
            String finalTempFile;
                finalTempFile = CommonUtils.generateTempFilePathAndName(pipelineRunId, ApplicationConstants.Sources.GAM, ApplicationConstants.GamReportTypes.DELIVERY.name());
            List<Path> tarFiles = Lists.newArrayList();

            Boolean cleanupTempFile = pipelineConfig.apiconfig.source.cleanupTempFile;

            // Get refreshed Oauth token credential
            Credential credential = auth.get();

            // Begin the download for Orders
            CommonUtils.timerSplit(stopWatch, "Ingest Gam Orders from API");
            GamOrderRequest gamOrderRequest = new GamOrderRequest(startDate, endDate, tarFiles, pipelineRunId);
            log.info("Attempting to download Gam Order data from API");
            GamOrderResponse gamOrderResponse = (GamOrderResponse) orderService.get(gamOrderRequest, credential);
            tarFiles = gamOrderResponse.getTempFileList();
            log.debug("Total number of temp files landed after Order response: {}", tarFiles.size());

            //Begin the download for Line Item Delivery Metrics
            CommonUtils.timerSplit(stopWatch, "Ingest Gam Line Item Delivery Metrics from API");
            String orderIds = GamUtils.orderIdsToCommaString(gamOrderResponse);
            GamLineItemRequest gamLineItemRequest = new GamLineItemRequest(orderIds, startDate, endDate, tarFiles, pipelineRunId);
            log.info("Attempting to download Gam Line Item data from API");
            GamLineItemResponse gamLineItemResponse = (GamLineItemResponse) lineItemService.get(gamLineItemRequest,
                    credential);
            tarFiles = gamLineItemResponse.getTarFileList();
            log.debug("Total number of temp files landed after Line Item Delivery response: {}", tarFiles.size());

            // Write to HDFS
            CommonUtils.timerSplit(stopWatch, "Write to HDFS");
            log.info("Attempting to write downloaded Gam Delivery data to HDFS...");
            Boolean finalWriteSuccessful = writerService.writeToHDFS(targetFile,finalTempFile, tarFiles,
                    3 ,cleanupTempFile);
            if (Boolean.FALSE.equals(finalWriteSuccessful)) {
                throw new IOException("All write attempts to HDFS failed for " + targetFile);
            } else {
                log.info("Successfully wrote the downloaded file to hdfs : " + targetFile);
            }

        } catch (Exception e) {
            log.error("Exception thrown while retrieving results", e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }
}
