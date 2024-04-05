package com.target.kelsaapi.common.vo.pipeline.request;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.annotations.SerializedName;
import com.target.kelsaapi.common.vo.pipeline.state.PipelineRunState;
import lombok.Data;
import lombok.Getter;

import javax.annotation.Nullable;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PipelineRunRequest {

    protected final String source;

    @SerializedName("start_date")
    @JsonAlias("start_date")
    @Getter(onMethod_ = {@JsonGetter(value = "start_date")})
    protected final String startDate;

    @SerializedName("end_date")
    @JsonAlias("end_date")
    @Getter(onMethod_ = {@JsonGetter(value = "end_date")})
    protected final String endDate;

    @SerializedName("target_path")
    @JsonAlias("target_path")
    @Getter(onMethod_ = {@JsonGetter(value = "target_path")})
    protected final String targetPath;

    @SerializedName("report_type")
    @JsonAlias("report_type")
    @Getter(onMethod_ = {@JsonGetter(value = "report_type")})
    @Nullable
    private final String reportType;

    public PipelineRunRequest(PipelineRunState pipelineRunState) {
        this.source = pipelineRunState.getSourceSystem();
        this.startDate = pipelineRunState.getStartDate();
        this.endDate = pipelineRunState.getEndDate();
        this.targetPath = pipelineRunState.getLandingFile();
        this.reportType = pipelineRunState.getSourceReportType();
    }

    public PipelineRunRequest(String source, String startDate, String endDate, String targetPath,
                              @Nullable String reportType) {
        this.source = source;
        this.startDate = startDate;
        this.endDate = endDate;
        this.targetPath = targetPath;
        this.reportType = reportType;
    }
}
