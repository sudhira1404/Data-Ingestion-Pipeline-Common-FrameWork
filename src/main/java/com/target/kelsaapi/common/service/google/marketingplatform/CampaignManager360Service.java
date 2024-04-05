package com.target.kelsaapi.common.service.google.marketingplatform;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.dfareporting.Dfareporting;
import com.google.api.services.dfareporting.model.File;
import com.google.api.services.dfareporting.model.Report;
import com.google.api.services.dfareporting.model.UserProfile;
import com.google.api.services.dfareporting.model.UserProfileList;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.target.kelsaapi.common.exceptions.GoogleMarketingPlatformException;
import com.target.kelsaapi.common.vo.google.request.marketingplatform.CampaignManager360ReportRequest;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

/**
 * Service layer for interacting with the Campaign Manager 360 APIs
 */
@Slf4j
@Service
public class CampaignManager360Service implements CampaignManager360Interface {

    private final PipelineConfig.Google.MarketingPlatform config;

    private final GoogleApiServicesFactory service;

    /**
     * Constructor used by Spring Framework to bootstrap this service
     *
     * @param pipelineConfig The PipelineConfig configuration instance
     */
    @Autowired
    CampaignManager360Service(PipelineConfig pipelineConfig, GoogleApiServicesFactory service) {
        this.config = pipelineConfig.getApiconfig().getSource().getGoogle().getMarketingPlatform();
        this.service = service;
    }

    /**
     * Prepares a report request, runs it, and waits for the file to be ready to download.
     *
     * @param request The {@link CampaignManager360ReportRequest} object that has all fields set to generate a report request
     * @return The {@link InputStream} from the remote url where the csv file may be streamed from
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public InputStream getData(CampaignManager360ReportRequest request) throws GoogleMarketingPlatformException {

        try {
            ServiceAccountCredentials credential = service.getServiceAccountCredentials();
            HttpCredentialsAdapter init = service.initializeHttpRequest(credential);
            String applicationName = request.getAPPLICATION_NAME();

            //Initializes the reporting object, but does not yet create the report run request
            Dfareporting reporting = service.getDfareportingService(init, applicationName);
            long profileId = getProfileId(reporting);

            //Save as a new report
            Report newReport = reporting.reports().insert(profileId, request.getReport()).execute();
            long reportId = newReport.getId();

            //Run the report
            File file = reporting.reports().run(profileId, reportId).execute();

            //Wait for the report to be finished
            wait(file, reporting);
            return getDownloadStream(file, reporting);
        } catch (IOException e) {
            throw new GoogleMarketingPlatformException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Fetches the Profile ID from the {@link Dfareporting} session object.
     *
     * @param reporting The {@link Dfareporting} session object.
     * @return The Profile ID.
     * @throws IOException
     * @throws RuntimeException
     */
    private long getProfileId(Dfareporting reporting) throws IOException, RuntimeException {
        // Retrieve and print all user profiles for the current authorized user.
        UserProfileList profiles = reporting.userProfiles().list().execute();
        long profileId = 0;
        log.debug("# Profiles: " + profiles.getItems().size());
        for (int i = 0; i < profiles.getItems().size(); i++) {
            UserProfile profile = profiles.getItems().get(i);
            log.debug("User name: " + profile.getUserName());
            log.debug("Profile ID: " + profile.getProfileId());
            log.debug("Account Name: " + profile.getAccountName());
            log.debug("Account ID: " + profile.getAccountId());
            log.debug("ETag: " + profile.getEtag());
            log.debug("Kind: " + profile.getKind());
            log.debug("Sub account name: " + profile.getSubAccountName());
            log.debug("Sub account id: " + profile.getSubAccountId());
            if (profile.getUserName().equals(config.getUserName())) {
                profileId = profile.getProfileId();
            } else {
                log.warn("Profile returned from API {} does not match username from configs {}",
                        profile.getProfileId(), config.getUserName());
            }
        }
        if (profiles.isEmpty()) throw new RuntimeException("No valid Profile ID found matching username from configs");
        return profileId;
    }

    /**
     * Waits for the remote file to be ready to download. Uses {@link ExponentialBackOff} to avoid repeating calls
     * to the API too frequently, which could cause a rate-limiter issue.
     *
     * @param file The {@link File} to wait for.
     * @param reporting The {@link Dfareporting} session object.
     * @return True if the remote file is ready to download; False if it times out waiting.
     * @throws IOException
     * @throws InterruptedException
     */
    private Boolean wait(File file, Dfareporting reporting) throws IOException {
        long fileId = file.getId();
        long reportId = file.getReportId();
        BackOff backOff =
                new ExponentialBackOff.Builder()
                        .setInitialIntervalMillis(10 * 1000) // 10 second initial retry
                        .setMaxIntervalMillis(10 * 60 * 1000) // 10 minute maximum retry
                        .setMaxElapsedTimeMillis(120 * 60 * 1000) // 2 hour total retry
                        .build();

        do {
            file = reporting.files().get(reportId, fileId).execute();

            // List of File Statuses: PROCESSING, REPORT_AVAILABLE, FAILED, CANCELLED, QUEUED
            if ("REPORT_AVAILABLE".equals(file.getStatus())) {
                // File has finished processing.
                log.info("File status is {}, ready to download.", file.getStatus());
                return true;
            } else if ( "FAILED".equals(file.getStatus()) || "CANCELLED".equals(file.getStatus()) ) {
                // File failed to process.
                log.error("File status is {}, processing failed.", file.getStatus());
                return false;
            }

            // The file hasn't finished processing yet, wait before checking again.
            long retryInterval = backOff.nextBackOffMillis();
            if (retryInterval == BackOff.STOP) {
                log.error("File processing deadline exceeded.");
                return false;
            }

            log.info("File status is {}, sleeping for {}", file.getStatus(), retryInterval);
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                return false;
            }
        } while (true);
    }

    /**
     * Retrieves the downloadable file as an {@link InputStream} object.
     *
     * @param file The {@link File} to download.
     * @param reporting The {@link Dfareporting} session object.
     * @return
     * @throws IOException
     */
    private InputStream getDownloadStream(File file, Dfareporting reporting) throws IOException {
        long fileId = file.getId();
        long reportId = file.getReportId();

        Dfareporting.Files.Get getRequest = reporting.files().get(reportId, fileId);
        return getRequest.executeMediaAsInputStream();
    }
}
