package com.target.kelsaapi.common.vo.google.response.admanager.delivery;

import com.google.api.ads.admanager.axis.v202311.LineItem;
import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import com.target.kelsaapi.common.vo.google.state.admanager.delivery.GamForecastableLineItems;
import com.target.kelsaapi.common.vo.google.state.admanager.delivery.GamForecastableLineItemsId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@EqualsAndHashCode(callSuper = false)
@Slf4j
public class GamLineItemResponse extends GamResponse {

    @Getter
    private List<GamLineItem> lineItems;

    @Getter
    private LineItem[] originalLineItems;

    @Getter
    private String reportDate;

    @Getter
    private List<Path> tarFileList;

    @Getter
    private String pipelineRunId;

    @Getter
    private int requestLoopId;

    @Getter
    private Instant reportDateTime;

    @Getter
    private List<GamForecastableLineItems> lineItemStates;

    public GamLineItemResponse(LineItem[] lineItems, String reportDate, String pipelineRunId, int requestLoopId, Instant reportDateTime) {
        super();
        log.debug("Initialized GamLineItemResponse starting at {}",reportDateTime);

        this.originalLineItems = lineItems;
        this.reportDate = reportDate;
        log.debug("Initialized field reportDate : {}", getReportDate());
        this.pipelineRunId = pipelineRunId;
        this.requestLoopId = requestLoopId;
        this.reportDateTime = reportDateTime;
        this.setLineItems(lineItems, reportDate, reportDateTime);
    }

    public GamLineItemResponse(List<Path> tarFileList) {
        super();
        log.info("Initialize final response with temp files list to be included in tar");
        this.tarFileList = tarFileList;
    }

    public void setLineItems(LineItem[] lineItems, String reportDate, Instant reportDateTime) {
        this.lineItems = Lists.newArrayList();
        List<String> jsonList = Lists.newArrayList();
        this.lineItemStates = Lists.newArrayList();

        log.debug("Received a total of {} line items from the API. Deserializing into the GamLineItem object now", lineItems.length);
        for (Iterator<LineItem> it = Arrays.stream(lineItems).iterator(); it.hasNext(); ) {
            LineItem li = it.next();
            GamLineItem gli = new GamLineItem(li, reportDate, reportDateTime);
            this.lineItems.add(gli);
            log.debug("Successfully added line item to list of deserialized GamLineItems");
            String jsonLineItem = gli.toJson();
            jsonList.add(jsonLineItem);
            log.debug("Successfully converted Line Item to Json string");
            this.lineItemStates.add(toGamLineItemDeliveryState(li.getId(),reportDate));
            this.setResponseList(jsonList);
        }
        log.debug("Total deserialized line items in the GamResponse object now : {}", this.getResponseList().size());
    }

    private GamForecastableLineItems toGamLineItemDeliveryState(Long id, String startDate) {
        return new GamForecastableLineItems(new GamForecastableLineItemsId(id, startDate));
    }
}
