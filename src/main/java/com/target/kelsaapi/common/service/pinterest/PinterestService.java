package com.target.kelsaapi.common.service.pinterest;

import com.target.kelsaapi.common.vo.Oauth;
import com.target.kelsaapi.common.vo.pinterest.PinterestReportRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface PinterestService {
    List<Path> getPinterestData(PinterestReportRequest request, Oauth oauth, String pipelineRunId, String reportType) throws IOException;
}
