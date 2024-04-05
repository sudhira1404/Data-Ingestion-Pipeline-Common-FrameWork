package com.target.kelsaapi.common.vo.google.request.marketingplatform;

import lombok.Data;

@Data
public abstract class MarketingPlatformRequest {

    protected final String REPORT_FORMAT = "CSV";

    protected final String APPLICATION_NAME = "marketing-data-foundation";

    protected String startDate;

    protected String endDate;

    protected String applicationName;

}
