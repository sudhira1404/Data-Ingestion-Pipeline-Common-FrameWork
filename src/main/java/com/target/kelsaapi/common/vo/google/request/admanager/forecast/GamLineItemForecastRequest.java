package com.target.kelsaapi.common.vo.google.request.admanager.forecast;

import com.google.api.ads.admanager.axis.v202311.AvailabilityForecastOptions;
import com.google.api.ads.admanager.axis.v202311.DeliveryForecastOptions;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GamLineItemForecastRequest extends GamRequest {

    @Getter
    AvailabilityForecastOptions availabilityForecastOptions;

    @Getter
    DeliveryForecastOptions deliveryForecastOptions;

    public GamLineItemForecastRequest(String startDate, String endDate) {
        super(startDate, endDate);
        setAvailabilityForecastOptions();
        setDeliveryForecastOptions();
    }

    private void setAvailabilityForecastOptions() {
        this.availabilityForecastOptions = new AvailabilityForecastOptions();
        this.availabilityForecastOptions.setIncludeContendingLineItems(true);
        this.availabilityForecastOptions.setIncludeTargetingCriteriaBreakdown(false);
        log.debug("Successfully initialized Availability Forecast options {}", this.availabilityForecastOptions.toString());
    }

    private void setDeliveryForecastOptions() {
        this.deliveryForecastOptions = new DeliveryForecastOptions();
    }
}
