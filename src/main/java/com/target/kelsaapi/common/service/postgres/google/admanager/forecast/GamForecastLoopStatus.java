package com.target.kelsaapi.common.service.postgres.google.admanager.forecast;

public interface GamForecastLoopStatus {
    Integer getRunningTotal();
    Integer getCompletedTotal();
    Integer getFailedTotal();
    Integer getInitializedTotal();
}
