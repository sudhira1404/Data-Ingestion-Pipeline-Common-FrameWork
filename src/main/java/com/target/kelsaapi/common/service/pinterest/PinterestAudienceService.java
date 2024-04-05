package com.target.kelsaapi.common.service.pinterest;

import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequest;

import java.io.IOException;
import java.util.List;

public interface PinterestAudienceService {
    List<String> getPinterestAudienceData(PinterestReportRequest request, Oauth oauth, String pipelineRunId, String reportType) throws IOException;
}
