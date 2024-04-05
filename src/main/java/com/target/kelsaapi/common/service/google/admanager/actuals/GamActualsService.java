package com.target.kelsaapi.common.service.google.admanager.actuals;

import com.google.api.ads.admanager.axis.utils.v202311.ReportDownloader;
import com.google.api.ads.admanager.axis.v202311.ReportJob;
import com.google.api.ads.admanager.axis.v202311.ReportQuery;
import com.google.api.ads.admanager.axis.v202311.ReportServiceInterface;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.errorprone.annotations.DoNotCall;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.google.admanager.AdManagerSessionServicesFactoryInterface;
import com.target.kelsaapi.common.service.google.admanager.GamServiceInterface;
import com.target.kelsaapi.common.util.CommonUtils;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import com.target.kelsaapi.common.vo.google.request.admanager.actuals.GamActualsRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.actuals.GamActualsResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.RemoteException;

/**
 * A service layer that interacts with the Google Ad Manager SDK to download Gam Actuals data
 */
@Slf4j
@Service
public class GamActualsService implements GamServiceInterface {

    private final AdManagerSessionServicesFactoryInterface adManagerServices;

    /**
     * The constructor used by Spring Framework to spin up and autowire this singleton service
     *
     * @param adManagerServices The {@link AdManagerSessionServicesFactoryInterface} for initializing Ad Manager Services from
     */
    @Autowired
    public GamActualsService(AdManagerSessionServicesFactoryInterface adManagerServices) {

        this.adManagerServices = adManagerServices;

    }

    /**
     * Uses the {@link ReportServiceInterface} to run a {@link ReportJob} that results in a downloadable dataset
     * which contains the Gam Actuals data requested in the provided {@link ReportQuery}. The data is downloaded
     * via a {@link ReportDownloader} and provided back to the caller in a {@link GamActualsResponse}
     *
     * @param query The {@link ReportQuery} containing the filter conditions to retrieve Gam Actuals reporting data using.
     * @param credential A refreshed Google {@link Credential} object.
     * @return A new {@link GamActualsResponse} object containing the results set.
     * @throws GamException A wrapper exception class containing the message and cause from the underlying exception thrown.
     */
    private GamActualsResponse getGamActualsData(ReportQuery query, Credential credential) throws GamException, InterruptedException {

        try {
            // Create report downloader.
            ReportDownloader reportDownloader = retryableRunReport(credential, query);

            // Initialize response object
            GamActualsResponse response = new GamActualsResponse();

            // If this is a request with the empty postal code query in it, make only a single attempt
            if (query.getStatement().getQuery().equals(ApplicationConstants.EMPTY_POSTAL_CODE_GAM_QUERY)) {
                try {
                    response.setDownloadableURL(GamUtils.downloadReportToURL(reportDownloader));
                } catch (MalformedURLException | RemoteException e) {
                    throw new GamException("Unable to retrieve results from the empty postal code query. Exception cause: ", e.getCause());
                }
            } else {
                //When the request is using zip code lists, we'll use an exponential backoff strategy for retries up to the max allowed time
                retryableDownloadReport(response, reportDownloader);
            }
            return response;

        } catch (GamException | ValidationException | IOException e) {
            throw new GamException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public GamResponse get(GamRequest request, Credential credential) throws GamException {

        GamActualsRequest actualsRequest = (GamActualsRequest) request;

        try {
            return getGamActualsData(actualsRequest.getReportQueryObject(), credential);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Deprecated
    @DoNotCall
    @Override
    public GamResponse get(Credential credential) throws GamException {
        return null;
    }

    @Deprecated
    @DoNotCall
    @Override
    public Credential get() throws GamException {
        return null;
    }

    private ReportDownloader retryableRunReport(Credential credential, ReportQuery query)
            throws IOException, GamException, InterruptedException, ValidationException {
        int attempt = 1;
        BackOff backOff = CommonUtils.startBackOff();
        long retryInterval = backOff.nextBackOffMillis();
        boolean retry=true;
        boolean isSuccessful=false;
        ReportDownloader reportDownloader;
        do {
            log.info("Attempt {} to download GAM Actuals data",attempt);

                // Generate a new session
                AdManagerSession session = adManagerServices.initAdManagerSession(credential);

                // Get the ReportService.
                ReportServiceInterface reportService = adManagerServices.initReportService(session);
                log.info("Initialized Gam Reporting Services");

                // Initialize the ReportJob
                ReportJob reportJob = adManagerServices.initReportJob(query);
                log.info("Initialized Gam Actuals Report Job");

                // Run the ReportJob
                ReportJob runningReport = adManagerServices.runReportJob(reportService, reportJob);
                log.info("Begin running Gam Actuals Report Job");

                // Create report downloader.
                reportDownloader = adManagerServices.initReportDownloader(reportService, runningReport);
            try {
                // Blocks this thread until report is ready to download
                isSuccessful = reportDownloader.waitForReportReady();
            } catch (Exception te) {
                log.error(te.getMessage(), te.getCause());
            }
            if (isSuccessful) {
                log.info("Report request was successful on attempt number {}.",attempt);
                retry=false;
            } else {
                log.warn("Report Request was not successful on attempt {}.", attempt);
                if (retryInterval == BackOff.STOP) {
                    throw new GamException("Unable to request report after max wait time exceeded!");
                } else {
                    log.warn("Will sleep for {} milliseconds before retrying",retryInterval);
                    Thread.sleep(retryInterval);
                    attempt++;
                    retryInterval=backOff.nextBackOffMillis();
                }
            }
        } while (retry);
        return reportDownloader;
    }

    private void retryableDownloadReport(GamActualsResponse response, ReportDownloader reportDownloader)
            throws IOException, GamException, InterruptedException {
        int attempt = 1;
        BackOff backOff = CommonUtils.startBackOff();
        long retryInterval = backOff.nextBackOffMillis();
        boolean retry=true;
        do {
            log.info("Attempt {} to download GAM Actuals data",attempt);
            try {
                response.setResponseList(GamUtils.downloadReportToList(reportDownloader));
            } catch (Exception te) {
                log.error(te.getMessage(), te.getCause());
            }
            if (response.getResponseList().isEmpty()) {
                Pair<Integer,Long> wait = GamUtils.wait(attempt, retryInterval, backOff);
                attempt = wait.getFirst();
                retryInterval = wait.getSecond();
            } else {
                log.info("Successfully downloaded {} results on attempt number {}.",
                        response.getResponseList().size(),attempt);
                retry=false;
            }
        } while (retry);
    }
}
