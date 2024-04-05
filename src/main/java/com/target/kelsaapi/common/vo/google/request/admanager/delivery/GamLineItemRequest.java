package com.target.kelsaapi.common.vo.google.request.admanager.delivery;

import com.google.api.ads.admanager.axis.utils.v202311.StatementBuilder;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.file.Path;
import java.util.List;

@EqualsAndHashCode(callSuper=false)
@Data
public class GamLineItemRequest extends GamRequest {

    private StatementBuilder lineItemStatementBuilder;

    private List<Path> tempFileList;

    private String pipelineRunId;

    public GamLineItemRequest(String orderIds, String startDate, String endDate, List<Path> tempFileList, String pipelineRunId) {

        super(startDate, endDate);

        this.lineItemStatementBuilder = GamUtils.generateLineItemStatement(orderIds);

        this.tempFileList = tempFileList;

        this.pipelineRunId = pipelineRunId;
    }

}
