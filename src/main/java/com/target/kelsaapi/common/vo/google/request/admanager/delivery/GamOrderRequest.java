package com.target.kelsaapi.common.vo.google.request.admanager.delivery;

import com.google.api.ads.admanager.axis.v202311.Statement;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.file.Path;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class GamOrderRequest extends GamRequest {

    private Statement statement;

    private List<Path> tempFileList;

    private String pipelineRunId;

    public GamOrderRequest(String startDate, String endDate, List<Path> tempFileList, String pipelineRunId) {
        super(startDate, endDate);
        this.statement = GamUtils.generateOrderStatement(startDate, endDate);
        this.tempFileList = tempFileList;
        this.pipelineRunId = pipelineRunId;
    }
}
