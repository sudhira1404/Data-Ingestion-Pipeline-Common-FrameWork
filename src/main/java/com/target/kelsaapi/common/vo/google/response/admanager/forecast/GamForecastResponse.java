package com.target.kelsaapi.common.vo.google.response.admanager.forecast;

import com.google.api.ads.admanager.axis.v202311.ForecastService;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.Instant;

/**
 * This abstract class is the basis for the two kinds of responses which may come from the {@link ForecastService}.
 * This contains common fields and accessors to be able to retrieve a deserialized response or error message.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public abstract class GamForecastResponse extends GamResponse {

    private String reportDate;

    private int attempts;

    private String forecastCreateTimestamp;

    private String jsonForecast;

    private String failureReason;

    public GamForecastResponse(String reportDate, int attempts) {
        this.reportDate = reportDate;
        this.attempts = attempts;
        setForecastCreateTimestamp();
    }

    public void setForecastCreateTimestamp() {
        this.forecastCreateTimestamp = GamUtils.instantToCentralZoneString(Instant.now());
    }

}
