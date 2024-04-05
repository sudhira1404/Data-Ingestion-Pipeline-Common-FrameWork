package com.target.kelsaapi.common.service.salesforce;

import com.target.kelsaapi.common.vo.Oauth;

import java.io.IOException;
import java.util.List;

public interface SalesforceService {
    List<String> getSalesforceObjectData(Oauth oauth, String pipelineRunId, String reportType) throws IOException;
}
