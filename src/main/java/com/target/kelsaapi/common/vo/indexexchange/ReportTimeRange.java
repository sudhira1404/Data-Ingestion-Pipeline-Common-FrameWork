package com.target.kelsaapi.common.vo.indexexchange;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReportTimeRange {

    @JsonProperty("from")
    public String from;
    @JsonProperty("to")
    public String to;
}