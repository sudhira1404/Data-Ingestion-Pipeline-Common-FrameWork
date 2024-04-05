package com.target.kelsaapi.common.vo.tradedesk;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ReportDelivery {
    @JsonProperty("ReportDestination")
    public String reportDestination;
    @JsonProperty("DeliveredPath")
    public String deliveredPath;
    @JsonProperty("DeliveredUTC")
    public String deliveredUTC;
    @JsonProperty("DownloadURL")
    public String downloadURL;
    @JsonProperty("DownloadURLExpirationUTC")
    public String downloadURLExpirationUTC;
}
