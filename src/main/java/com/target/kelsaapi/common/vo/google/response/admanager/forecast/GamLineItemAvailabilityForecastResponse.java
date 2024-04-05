package com.target.kelsaapi.common.vo.google.response.admanager.forecast;

import com.google.api.ads.admanager.axis.v202311.*;
import com.google.api.client.util.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.util.GamUtils;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This concrete class takes the {@link AvailabilityForecast} and transforms it into a GamAvailabilityForecast POJO.
 * The serialized json of this pojo resulting from a successful response may be fetched through {@code getJsonForecast()}.
 * The error message from a failed response may be fetched through {@code getFailureReason()}.
 */
@Slf4j
public class GamLineItemAvailabilityForecastResponse extends GamForecastResponse {

    @Nullable
    @Getter
    private final AvailabilityForecast availabilityForecast;

    /**
     * Constructor for a new GamLineItemAvailabilityForecastResponse object.
     *
     * @param availabilityForecast The {@link AvailabilityForecast} from a successful Availability Forecast request response.
     * @param reportDate The start date in yyyy-mm-dd format.
     * @param attempts The number of attempts made up to this success.
     */
    public GamLineItemAvailabilityForecastResponse(@Nullable AvailabilityForecast availabilityForecast, String reportDate, int attempts, @Nullable String errorMessage) throws GamException {
        super(reportDate, attempts);
        if (availabilityForecast != null) {
            this.availabilityForecast = availabilityForecast;
            this.setJsonForecast(availabilityForecastToJson());
            this.setResponseList(List.of(this.getJsonForecast()));
        } else if (errorMessage != null) {
            this.availabilityForecast = null;
            this.setFailureReason(errorMessage);
        } else {
            throw new GamException("Error initializing GamLineItemAvailabilityForecastResponse! Must pass either an AvailabilityForecast OR an errorMessage");
        }
    }

    /**
     * Used to serialize the GamAvailabilityForecast POJO to a json string.
     * @return The json string from serializing the GamAvailabilityForecast.
     */
    private String availabilityForecastToJson() {
        Gson gson = new GsonBuilder().create();
        return gson.toJson(new GamAvailabilityForecast(this.getAvailabilityForecast(), this.getReportDate(), this.getForecastCreateTimestamp()));
    }

    /**
     * This POJO is the blueprint for the eventual serialized json which is stored in the jsonForecast field.
     */
    @Data
    static class GamAvailabilityForecast {
        private Long lineItemId;
        private Long orderId;
        private String unitType;
        private Long availableUnits;
        private Long deliveredUnits;
        private Long matchedUnits;
        private Long possibleUnits;
        private Long reservedUnits;
        private ArrayList<GamBreakdown> breakdowns;
        private TargetingCriteriaBreakdown[] targetingCriteriaBreakdowns;
        private ContendingLineItem[] contendingLineItems;
        private ArrayList<GamAlternativeForecast> alternativeUnitTypeForecasts;
        private String gamRecordType = "AVAILABILITY_FORECAST";
        private String forecastCreateTimestamp;
        private String reportDate;

        /**
         * Constructor for the GamAvailabilityForecast POJO.
         *
         * @param forecast The {@link AvailabilityForecast} from a successful Availability Forecast request response. This is converted into this POJO.
         * @param reportDate
         */
        public GamAvailabilityForecast(AvailabilityForecast forecast, String reportDate, String forecastCreateTimestamp) {

            this.setLineItemId(forecast.getLineItemId());
            this.setOrderId(forecast.getOrderId());
            this.setUnitType(forecast.getUnitType().getValue());
            this.setAvailableUnits(forecast.getAvailableUnits());
            this.setDeliveredUnits(forecast.getDeliveredUnits());
            this.setMatchedUnits(forecast.getMatchedUnits());
            this.setPossibleUnits(forecast.getPossibleUnits());
            this.setReservedUnits(forecast.getReservedUnits());
            this.setBreakdowns(forecast.getBreakdowns());
            this.setTargetingCriteriaBreakdowns(forecast.getTargetingCriteriaBreakdowns());
            this.setContendingLineItems(forecast.getContendingLineItems());
            this.setAlternativeUnitTypeForecasts(forecast.getAlternativeUnitTypeForecasts());
            this.forecastCreateTimestamp = forecastCreateTimestamp;
            this.reportDate = reportDate;
        }

        /**
         * Converts a {@link ForecastBreakdown} to GamBreakdown POJO
         */
        @Data
        static class GamBreakdown {
            private String forecastBreakdownStartTime;
            private String forecastBreakdownEndTime;
            private GamBreakdownEntry[] forecastBreakdownEntries;

            /**
             * Constructor for a GamBreakdown POJO
             *
             * @param forecastBreakdown The {@link ForecastBreakdown} to be converted to a GamBreakdown
             */
            public GamBreakdown(ForecastBreakdown forecastBreakdown) {
                setForecastBreakdownStartTime(GamUtils.dateTimeToString(forecastBreakdown.getStartTime()));
                setForecastBreakdownEndTime(GamUtils.dateTimeToString(forecastBreakdown.getEndTime()));
                setForecastBreakdownEntries(forecastBreakdown.getBreakdownEntries());
            }


            /**
             * Used as a blueprint for converting a {@link ForecastBreakdownEntry} to a simpler POJO
             */
            @Data
            static class GamBreakdownEntry {
                private String forecastBreakdownEntryName;
                private Map<String,Long> forecastBreakdownForecast;
            }

            /**
             * Takes an array of {@link ForecastBreakdownEntry} and converts it to an array of GamBreakdownEntry POJOs
             *
             * @param breakdownEntries An array of {@link ForecastBreakdownEntry} to be converted to an array of GamBreakdownEntry POJOs
             */
            public void setForecastBreakdownEntries(ForecastBreakdownEntry[] breakdownEntries) {
                int max = breakdownEntries.length;
                this.forecastBreakdownEntries = new GamBreakdownEntry[max];
                int i = 0;
                for (ForecastBreakdownEntry entry : breakdownEntries) {
                    this.forecastBreakdownEntries[i].setForecastBreakdownEntryName(entry.getName());
                    BreakdownForecast bf = entry.getForecast();
                    Map<String,Long> map = Stream.of(new Object[][]{
                            {"matched",bf.getMatched()},
                            {"available",bf.getAvailable()},
                            {"possible",bf.getPossible()}
                    }).collect(Collectors.toMap(p -> (String)p[0], p -> (Long)p[1]));
                    this.forecastBreakdownEntries[i].setForecastBreakdownForecast(map);
                    i++;
                }
            }
        }

        /**
         * A setter that takes an array of {@link ForecastBreakdown} and converts it to a list of GamBreakdown POJOs
         *
         * @param forecastBreakdowns An array of {@link ForecastBreakdown} to be converted to a list of GamBreakdown POJOs
         */
        private void setBreakdowns(ForecastBreakdown[] forecastBreakdowns) {
            this.breakdowns = Lists.newArrayList();
            if (forecastBreakdowns != null) {
                for (ForecastBreakdown breakdown : forecastBreakdowns) {
                    this.breakdowns.add(new GamBreakdown(breakdown));
                }
                log.debug("Added {} forecast breakdowns to the Availability Forecast",this.breakdowns.size());
            } else {
                log.debug("No Forecast Breakdowns detected");
            }
        }

        /**
         * Used as a blueprint for converting a {@link AlternativeUnitTypeForecast} to a simpler POJO
         */
        @Data
        static class GamAlternativeForecast {
            private String unitType;
            private Long matchedUnits;
            private Long availableUnits;
            private Long possibleUnits;

            /**
             * Constructor of GamAlternativeForecast POJO
             *
             * @param alternativeUnitTypeForecast A {@link AlternativeUnitTypeForecast} to be converted to a GamAlternativeForecast POJO
             */
            GamAlternativeForecast(AlternativeUnitTypeForecast alternativeUnitTypeForecast) {
                this.unitType = alternativeUnitTypeForecast.getUnitType().getValue();
                this.matchedUnits = alternativeUnitTypeForecast.getMatchedUnits();
                this.availableUnits = alternativeUnitTypeForecast.getAvailableUnits();
                this.possibleUnits = alternativeUnitTypeForecast.getPossibleUnits();
            }
        }

        /**
         * A setter that takes an array of {@link AlternativeUnitTypeForecast} and converts it to a list of GamAlternativeForecast POJOs
         *
         * @param alternativeUnitTypeForecasts An array of {@link AlternativeUnitTypeForecast} to be converted to a list of GamAlternativeForecast POJOs
         */
        private void setAlternativeUnitTypeForecasts(AlternativeUnitTypeForecast[] alternativeUnitTypeForecasts) {
            this.alternativeUnitTypeForecasts = Lists.newArrayList();
            if (alternativeUnitTypeForecasts != null) {
                for (AlternativeUnitTypeForecast forecast : alternativeUnitTypeForecasts) {
                    this.alternativeUnitTypeForecasts.add(new GamAlternativeForecast(forecast));
                }
                log.debug("Added {} Alternative Unit Type Forecasts to the Availability Forecast",this.alternativeUnitTypeForecasts.size());
            } else {
                log.debug("No Alternative Unit Type Forecasts detected");
            }
        }
    }
}
