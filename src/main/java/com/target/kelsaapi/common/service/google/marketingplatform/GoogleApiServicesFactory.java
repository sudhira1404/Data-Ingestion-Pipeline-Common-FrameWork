package com.target.kelsaapi.common.service.google.marketingplatform;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.dfareporting.Dfareporting;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.target.kelsaapi.common.exceptions.GoogleMarketingPlatformException;
import com.target.kelsaapi.pipelines.config.PipelineConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.InputStream;

@Service
@Slf4j
public class GoogleApiServicesFactory implements GoogleApiServicesFactoryInterface {

    PipelineConfig.Google.MarketingPlatform config;

    @Autowired
    public GoogleApiServicesFactory(PipelineConfig config) {
        this.config = config.getApiconfig().getSource().getGoogle().getMarketingPlatform();
    }

    @Override
    public ServiceAccountCredentials getServiceAccountCredentials() throws GoogleMarketingPlatformException {

        try (InputStream is = new FileInputStream(config.jsonKeyFilePath)) {
            log.info("Initializing service account credentials from secret file : {}", config.jsonKeyFilePath);
            ServiceAccountCredentials sac = ServiceAccountCredentials.fromStream(is);
            log.info("Initialization of service account credentials successful");
            log.debug("Client id : {}", sac.getClientId());
            return sac;
        } catch (Exception e) {
            throw new GoogleMarketingPlatformException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public HttpCredentialsAdapter initializeHttpRequest(ServiceAccountCredentials credentials)
            throws GoogleMarketingPlatformException {

        try {
            log.info("Initializing Http request object");
            HttpCredentialsAdapter hca = new HttpCredentialsAdapter(credentials);
            log.info("Successfully initialized Http request object");
            return hca;
        } catch (Exception e) {
            throw new GoogleMarketingPlatformException(e.getMessage(), e.getCause());
        }

    }

    @Override
    public Dfareporting getDfareportingService(HttpCredentialsAdapter requestInitializer,
                                               String applicationName)
            throws GoogleMarketingPlatformException {

        try {
            JsonFactory jsonFactory = new GsonFactory();
            log.info("JsonFactory initialized ");
            NetHttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
            log.info("NetHttpTransport initialized");

            Dfareporting dfareporting = new Dfareporting.Builder(transport, jsonFactory, requestInitializer)
                    .setApplicationName(applicationName)
                    .build();
            log.info("Initialized Dfareporting session");

            return dfareporting;
        } catch (Exception e) {
            throw new GoogleMarketingPlatformException(e.getMessage(), e.getCause());
        }
    }
}
