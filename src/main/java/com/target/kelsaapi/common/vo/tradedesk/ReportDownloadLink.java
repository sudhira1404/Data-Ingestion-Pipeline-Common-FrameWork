package com.target.kelsaapi.common.vo.tradedesk;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class ReportDownloadLink {

    @JsonProperty("Result")
    public List<ReportResult> result;
    @JsonProperty("ResultCount")
    public int resultCount;
    @JsonProperty("TotalFilteredCount")
    public int totalFilteredCount;

    public ReportDelivery[] reportDelivery;
}
