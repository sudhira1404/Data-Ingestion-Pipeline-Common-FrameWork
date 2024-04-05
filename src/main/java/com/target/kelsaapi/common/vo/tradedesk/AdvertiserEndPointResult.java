package com.target.kelsaapi.common.vo.tradedesk;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdvertiserEndPointResult {

    @JsonProperty("Result")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<Advertiser> result;

    @JsonProperty("ResultCount")
    public int resultCount;

    @JsonProperty("TotalFilteredCount")
    public int totalFilteredCount;

    @JsonProperty("TotalUnfilteredCount")
    public int totalUnfilteredCount;
}
