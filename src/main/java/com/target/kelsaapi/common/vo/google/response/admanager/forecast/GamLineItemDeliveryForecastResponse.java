package com.target.kelsaapi.common.vo.google.response.admanager.forecast;

import com.google.api.ads.admanager.axis.v202311.DeliveryForecast;
import com.google.api.ads.admanager.axis.v202311.LineItemDeliveryForecast;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.target.kelsaapi.common.exceptions.GamException;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * This concrete class takes the {@link DeliveryForecast} and transforms it into a GamLineItemDeliveryForecast pojo.
 * The serialized json of this pojo resulting from a successful response may be fetched through {@code getJsonForecast()}.
 * The error message from a failed response may be fetched through {@code getFailureReason()}.
 */
public class GamLineItemDeliveryForecastResponse extends GamForecastResponse {

    @Nullable
    @Getter
    private final LineItemDeliveryForecast[] forecasts;

    @Getter
    private List<Pair<Long,String>> savableGamForecasts;

    @Getter
    private final long[] requestedLineItems;

    @Getter
    @Setter
    private long[] missingLineItems;

    /**
     * Primary constructor used when a valid DeliveryForecast object is initialized from the ForecastService request.
     *
     * @param forecasts This is the array of {@link LineItemDeliveryForecast} objects retrieved from the ForecastService request.
     * @param reportDate In YYYY-MM-DD format
     * @param attempts The number of attempts made up to this success.
     * @param requestedLineItems The array of Line Item IDs this Delivery Forecast attempt was made against.
     */
    public GamLineItemDeliveryForecastResponse(@Nullable LineItemDeliveryForecast[] forecasts, String reportDate, int attempts, long[] requestedLineItems, @Nullable String errorMessage) throws GamException {
        super(reportDate, attempts);
        if (forecasts != null) {
            this.forecasts = forecasts;
            this.requestedLineItems = requestedLineItems;
            toJson();
        } else if (errorMessage != null) {
            this.forecasts = null;
            this.requestedLineItems = requestedLineItems;
            setFailureReason(errorMessage);
        } else {
            throw new GamException("Error initializing GamLineItemDeliveryForecastResponse! Must pass either an array of LineItemDeliveryForecast OR an errorMessage");
        }
    }

    private ArrayList<Long> lineItemArrayToList() {
        ArrayList<Long> returnableList = Lists.newArrayList();
        for (long id : this.requestedLineItems) {
            returnableList.add(id);
        }
        return returnableList;
    }


    private long[] lineItemListToArray(ArrayList<Long> lineItemList) {
        long[] returnableArray = new long[lineItemList.size()];
        int i = 0;
        for (long id : lineItemList) {
            returnableArray[i] = id;
            i++;
        }
        return returnableArray;
    }

    private void toJson() {
        Gson gson = new GsonBuilder().create();
        ArrayList<String> responses = Lists.newArrayList();
        this.savableGamForecasts = Lists.newArrayList();
        ArrayList<Long> requests = lineItemArrayToList();

        for (LineItemDeliveryForecast forecast : this.forecasts) {
            Long id = forecast.getLineItemId();
            requests.remove(id);
            String json = gson.toJson(new GamLineItemDeliveryForecast(forecast, this.getReportDate(), this.getForecastCreateTimestamp()));
            responses.add(json);
            this.savableGamForecasts.add(Pair.of(id,json));
        }
        if (requests.size()>0) {
            setMissingLineItems(lineItemListToArray(requests));
        }

        setResponseList(responses);
    }

    /**
     * This POJO is the blueprint for the eventual serialized json which is stored in the jsonForecast field.
     */
    @Data
    static class GamLineItemDeliveryForecast {
        Long lineItemId;
        Long orderId;
        Long deliveredUnits;
        Long matchedUnits;
        Long predictedDeliveryUnits;
        String unitType;
        String gamRecordType = "DELIVERY_FORECAST";
        String forecastCreateTimestamp;
        String reportDate;

        /**
         * Constructor for the GamLineItemDeliveryForecast POJO
         * @param forecast The {@link LineItemDeliveryForecast} to convert to this GamLineItemDeliveryForecast object.
         */
        GamLineItemDeliveryForecast (LineItemDeliveryForecast forecast, String reportDate, String forecastCreateTimestamp) {
            this.setLineItemId(forecast.getLineItemId());
            this.setOrderId(forecast.getOrderId());
            this.setDeliveredUnits(forecast.getDeliveredUnits());
            this.setMatchedUnits(forecast.getMatchedUnits());
            this.setPredictedDeliveryUnits(forecast.getPredictedDeliveryUnits());
            this.setUnitType(forecast.getUnitType().getValue());
            this.forecastCreateTimestamp = forecastCreateTimestamp;
            this.reportDate = reportDate;
        }
    }
}
