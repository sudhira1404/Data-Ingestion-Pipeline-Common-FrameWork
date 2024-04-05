package com.target.kelsaapi.common.vo.xandr;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ReportStatusResponse {

    @JsonProperty("status")
    String status;
    @JsonProperty("report")
    String report;
    @JsonProperty("execution_status")
    String execution_status;
    @JsonProperty("_was_this_status_cached_")
    int was_this_status_cached;
    @JsonProperty("dbg_info")
    DbgInfo dbg_info;

}

