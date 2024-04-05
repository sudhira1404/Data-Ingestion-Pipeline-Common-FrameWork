package com.target.kelsaapi.common.service.google.marketingplatform;

import com.google.api.services.dfareporting.Dfareporting;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.target.kelsaapi.common.exceptions.GoogleMarketingPlatformException;

public interface GoogleApiServicesFactoryInterface {

    ServiceAccountCredentials getServiceAccountCredentials() throws GoogleMarketingPlatformException;

    HttpCredentialsAdapter initializeHttpRequest(ServiceAccountCredentials credentials)
            throws GoogleMarketingPlatformException;

    Dfareporting getDfareportingService(HttpCredentialsAdapter requestInitializer,
                                        String applicationName)
            throws GoogleMarketingPlatformException;
}
