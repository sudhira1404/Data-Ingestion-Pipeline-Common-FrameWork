package com.target.kelsaapi.common.vo.tradedesk;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class ReportResult {
    @JsonProperty("ReportExecutionId")
    public int reportExecutionId;
    @JsonProperty("ReportExecutionState")
    public String reportExecutionState;
    @JsonProperty("LastStateChangeUTC")
    public String lastStateChangeUTC;
    @JsonProperty("DisabledReason")
    public Object disabledReason;
    @JsonProperty("Timezone")
    public String timezone;
    @JsonProperty("ReportStartDateInclusive")
    public String reportStartDateInclusive;
    @JsonProperty("ReportEndDateExclusive")
    public String reportEndDateExclusive;
    @JsonProperty("ReportScheduleId")
    public int reportScheduleId;
    @JsonProperty("ReportScheduleName")
    public String reportScheduleName;
    @JsonProperty("ReportDeliveries")
    public List<ReportDelivery> reportDeliveries;
}
