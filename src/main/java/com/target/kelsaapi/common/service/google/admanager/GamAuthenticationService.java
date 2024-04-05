package com.target.kelsaapi.common.service.google.admanager;

import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A service layer that interacts with the Google Ad Manager SDK to refresh Oauth tokens
 */
@Service
@Slf4j
public class GamAuthenticationService {

    private final Configuration config;

    private OfflineCredentials credentialBuilder;

    private Credential refreshedCredential;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    @Getter
    private final Lock r = rwl.readLock();

    @Getter
    private final Lock w = rwl.writeLock();

    /**
     * The constructor used by Spring Framework to spin up and autowire this singleton service
     *
     * @param pipelineConfig The {@link PipelineConfig} for retrieving the Google Account details from
     */
    @Autowired
    public GamAuthenticationService(PipelineConfig pipelineConfig) throws ValidationException, InterruptedException {
        this.config = pipelineConfig.getApiconfig().getSource().getGoogle().getAdManager().getGamConfig();
        setCredentialBuilder();
    }

    @Async(value = "gamCredentialRefreshMonitor")
    @Scheduled(fixedDelay = 240, timeUnit = TimeUnit.SECONDS)
    public void credentialRefreshRunner() throws InterruptedException {
        log.debug("Waking up");
        if (this.refreshedCredential == null || this.refreshedCredential.getExpiresInSeconds() < 300) {
            log.info("Time to refresh GAM Credentials as there are fewer than 5 minutes remaining on the current lease!");
            try {
                w.lockInterruptibly();
                setCredentials();
            } finally {
                w.unlock();
            }
        } else {
            log.debug("No need to refresh GAM credentials, remaining seconds : {}", this.refreshedCredential.getExpiresInSeconds());
        }
    }

    /**
     * Uses the {@link OfflineCredentials} object to generate a {@link Credential} object containing a newly
     * refreshed Oauth2 token.
     *
     * @return
     */
    private void setCredentials() {
        try {
            log.info("Attempting to refresh Gam credentials.");
            this.refreshedCredential = credentialBuilder.generateCredential();
            log.info("Gam credentials succesfully refreshed.");
            log.info("Refreshed token expires in {} seconds", this.refreshedCredential.getExpiresInSeconds());
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
            this.refreshedCredential = null;
        }
    }

    /**
     * Initializes the {@link OfflineCredentials} builder object from the {@link Configuration} object.
     * Meant to only be invoked at startup to read from the configured json file containing the credentials for
     * refreshing Oauth2 tokens with.
     *
     * @throws ValidationException
     */
    private void setCredentialBuilder() throws ValidationException {
        this.credentialBuilder = new OfflineCredentials.Builder()
                .forApi(OfflineCredentials.Api.AD_MANAGER)
                .from(config)
                .build();
    }

    public Credential get() throws GamException, InterruptedException {
        try {
            r.lockInterruptibly();
            Credential returnable = this.refreshedCredential;
            log.info("Skipping refresh of GAM credentials as there are {} seconds left on current token",returnable.getExpiresInSeconds());
            return returnable;
        } finally {
            r.unlock();
        }
    }

}
