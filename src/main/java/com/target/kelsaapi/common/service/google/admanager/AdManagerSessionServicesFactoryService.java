package com.target.kelsaapi.common.service.google.admanager;

import com.google.api.ads.admanager.axis.factory.AdManagerServices;
import com.google.api.ads.admanager.axis.utils.v202311.ReportDownloader;
import com.google.api.ads.admanager.axis.v202311.*;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import org.apache.commons.configuration.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.rmi.RemoteException;

@Service
public class AdManagerSessionServicesFactoryService implements AdManagerSessionServicesFactoryInterface {

    private final Configuration config;

    @Autowired
    protected AdManagerSessionServicesFactoryService(PipelineConfig pipelineConfig) {

        this.config = pipelineConfig.getApiconfig().getSource().getGoogle().getAdManager().getGamConfig();
    }

    @Override
    public AdManagerSession initAdManagerSession(Credential credential) throws ValidationException {
        return new AdManagerSession.Builder()
                .from(config)
                .withOAuth2Credential(credential)
                .build();
    }

    @Override
    public ReportServiceInterface initReportService(AdManagerSession session) {
        return initAdManagerServices().get(session, ReportServiceInterface.class);
    }

    @Override
    public PublisherQueryLanguageServiceInterface initPqlService(AdManagerSession session) {
        return initAdManagerServices().get(session, PublisherQueryLanguageServiceInterface.class);
    }

    @Override
    public OrderServiceInterface initOrderService(AdManagerSession session) {
        return initAdManagerServices().get(session, OrderServiceInterface.class);
    }

    @Override
    public LineItemServiceInterface initLineItemService(AdManagerSession session) {
        return initAdManagerServices().get(session, LineItemServiceInterface.class);
    }

    @Override
    public ReportJob initReportJob(ReportQuery reportQuery) {
        ReportJob reportJob = new ReportJob();
        reportJob.setReportQuery(reportQuery);
        return reportJob;
    }

    @Override
    public ReportJob runReportJob(ReportServiceInterface reportService, ReportJob reportJob) throws RemoteException {
        return reportService.runReportJob(reportJob);
    }

    @Override
    public ReportDownloader initReportDownloader(ReportServiceInterface reportService, ReportJob reportJob) {
        return new ReportDownloader(reportService, reportJob.getId());
    }

    @Override
    public ForecastServiceInterface initForecastServiceInterface(AdManagerSession session) {
        return initAdManagerServices().get(session, ForecastServiceInterface.class);
    }

    protected AdManagerServices initAdManagerServices() { return new AdManagerServices(); }



}
