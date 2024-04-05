package com.target.kelsaapi.common.vo.google.response.admanager.delivery;

import com.google.api.ads.admanager.axis.v202311.Order;
import com.google.api.client.util.Lists;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
@EqualsAndHashCode(callSuper=false)
@Data
public class GamOrderResponse extends GamResponse {

    private List<GamOrder> orders;

    private List<String> jsonOrders;

    private String reportDate;

    private List<Path> tempFileList;

    public GamOrderResponse(Order[] orders, String startDate, Instant reportTimestamp) {
        super();
        log.debug("Initialized GamOrderResponse");
        this.reportDate = startDate;
        log.debug("Initialized field reportDate : {}", getReportDate());
        this.setOrders(orders, startDate, reportTimestamp);
        log.debug("Used setter to initialize GamOrder list");
        this.setJsonOrders();
        log.debug("Used setter to initialize list of Json string orders");
        log.info("Total # orders returned : {}", jsonOrders.size());
    }

    public void setJsonOrders() {
        this.jsonOrders = Lists.newArrayList();

        log.debug("Now converting {} orders to a list of Json strings", this.getOrders().size());
        for (GamOrder order : this.getOrders()) {
            jsonOrders.add(order.toJson());
        }
        log.debug("The number of Json strings in the final list : {}", this.getJsonOrders().size());
    }

    public void setOrders(Order[] orders, String startDate, Instant reportTimestamp) {
        this.orders = Lists.newArrayList();

        log.debug("Received a total of {} orders from the API. Deserializing into the GamResponse object now", orders.length);
        for (Iterator<Order> it = Arrays.stream(orders).iterator(); it.hasNext(); ) {
            Order ord = it.next();
            log.debug("Order from Gam : {}", ord.toString());
            GamOrder go = new GamOrder(ord, startDate, reportTimestamp);
            log.debug("Order deserialized : {}", go.toString());
            this.orders.add(go);
            log.debug("Successfully added order to list of deserialized GamOrders");
        }
        log.debug("Total deserialized orders in the GamResponse object now : {}", this.getOrders().size());
    }
}
