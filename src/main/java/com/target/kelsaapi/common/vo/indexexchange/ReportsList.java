package com.target.kelsaapi.common.vo.indexexchange;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReportsList {

    @JsonProperty("fileID")
    public Integer fileID;
    @JsonProperty("reportID")
    public Integer reportID;
    @JsonProperty("reportTitle")
    public String reportTitle;
    @JsonProperty("reportTimeRange")
    public ReportTimeRange reportTimeRange;
    @JsonProperty("createdAt")
    public String createdAt;
    @JsonProperty("expiresAt")
    public String expiresAt;
    @JsonProperty("fileName")
    public String fileName;
    @JsonProperty("fileSize")
    public Integer fileSize;
    @JsonProperty("hash")
    public String hash;
    @JsonProperty("downloadURL")
    public String downloadURL;
    @JsonProperty("downloadStatus")
    public String downloadStatus;

}