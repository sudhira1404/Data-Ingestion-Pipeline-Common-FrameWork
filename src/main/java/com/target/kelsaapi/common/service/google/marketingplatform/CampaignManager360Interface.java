package com.target.kelsaapi.common.service.google.marketingplatform;

import com.target.kelsaapi.common.exceptions.GoogleMarketingPlatformException;
import com.target.kelsaapi.common.vo.google.request.marketingplatform.CampaignManager360ReportRequest;

import java.io.InputStream;

public interface CampaignManager360Interface {
    InputStream getData(CampaignManager360ReportRequest request) throws GoogleMarketingPlatformException;
}
