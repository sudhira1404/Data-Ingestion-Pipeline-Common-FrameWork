package com.target.kelsaapi.common.vo.xandr;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;


@Data
public class Report {

    @JsonProperty("report_type")
    Map<String, String> report_type;

    @JsonProperty("format")
    String format;


    @JsonProperty("report_interval")
    Map<String, String>  report_interval;

    @JsonProperty("columns")
    Map<String, String[]> columns;

}