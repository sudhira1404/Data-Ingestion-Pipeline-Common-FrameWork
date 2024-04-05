package com.target.kelsaapi.common.service.facebook;

import com.target.kelsaapi.common.exceptions.FacebookException;
import com.target.kelsaapi.common.vo.facebook.FacebookAdsInsightsRequest;
import com.target.kelsaapi.common.vo.facebook.FacebookAdsInsightsResponse;

public interface FacebookService {

    FacebookAdsInsightsResponse getApiData(FacebookAdsInsightsRequest facebookAdsInsightsRequest) throws FacebookException;

}