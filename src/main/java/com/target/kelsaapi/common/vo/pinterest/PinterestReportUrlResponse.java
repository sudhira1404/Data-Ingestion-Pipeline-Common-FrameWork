package com.target.kelsaapi.common.vo.pinterest;

import lombok.Data;

@Data
public class PinterestReportUrlResponse {
    private String status;
    private String code;
    private PinterestReportUrlResponseData data;
    private String message;
    private String endpoint_name;
}
