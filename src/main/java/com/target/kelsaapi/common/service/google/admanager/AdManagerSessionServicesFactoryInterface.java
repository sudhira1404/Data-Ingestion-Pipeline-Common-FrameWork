package com.target.kelsaapi.common.service.google.admanager;

import com.google.api.ads.admanager.axis.utils.v202311.ReportDownloader;
import com.google.api.ads.admanager.axis.v202311.*;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;

import java.rmi.RemoteException;

public interface AdManagerSessionServicesFactoryInterface {

    AdManagerSession initAdManagerSession(Credential credential) throws ValidationException;

    ReportServiceInterface initReportService(AdManagerSession session);

    PublisherQueryLanguageServiceInterface initPqlService(AdManagerSession session);

    OrderServiceInterface initOrderService(AdManagerSession session);

    LineItemServiceInterface initLineItemService(AdManagerSession session);

    ReportJob initReportJob(ReportQuery reportQuery);

    ReportJob runReportJob(ReportServiceInterface reportService, ReportJob reportJob) throws RemoteException;

    ReportDownloader initReportDownloader(ReportServiceInterface reportService, ReportJob reportJob);

    ForecastServiceInterface initForecastServiceInterface(AdManagerSession session);
}
