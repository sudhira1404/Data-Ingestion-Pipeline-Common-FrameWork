package com.target.kelsaapi.common.vo.xandr;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ReportStatus {

    @JsonProperty("Response")
    ReportStatusResponse reportStatusResponse;
}