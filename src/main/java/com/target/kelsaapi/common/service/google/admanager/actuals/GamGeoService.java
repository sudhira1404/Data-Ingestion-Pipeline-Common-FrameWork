package com.target.kelsaapi.common.service.google.admanager.actuals;

import com.google.api.ads.admanager.axis.utils.v202311.Pql;
import com.google.api.ads.admanager.axis.v202311.PublisherQueryLanguageServiceInterface;
import com.google.api.ads.admanager.axis.v202311.ResultSet;
import com.google.api.ads.admanager.lib.client.AdManagerSession;
import com.google.api.client.auth.oauth2.Credential;
import com.google.errorprone.annotations.DoNotCall;
import com.target.kelsaapi.common.exceptions.GamException;
import com.target.kelsaapi.common.service.google.admanager.AdManagerSessionServicesFactoryInterface;
import com.target.kelsaapi.common.service.google.admanager.GamServiceInterface;
import com.target.kelsaapi.common.util.GamUtils;
import com.target.kelsaapi.common.vo.google.request.admanager.GamRequest;
import com.target.kelsaapi.common.vo.google.response.admanager.GamResponse;
import com.target.kelsaapi.common.vo.google.response.admanager.actuals.GamGeoResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * A service layer that interacts with the Google Ad Manager SDK to retrieve Geographical data
 */
@Service
@Slf4j
public class GamGeoService implements GamServiceInterface {

    private final AdManagerSessionServicesFactoryInterface adManagerServices;

    /**
     * The constructor used by Spring Framework to spin up and autowire this singleton service
     *
     * @param adManagerServices The {@link AdManagerSessionServicesFactoryInterface} for initializing Ad Manager Services from
     */
    @Autowired
    public GamGeoService(AdManagerSessionServicesFactoryInterface adManagerServices) {

        this.adManagerServices = adManagerServices;
    }

    /**
     * Retrieves all Zip Codes from Gam's Geo_Target table via {@link Pql}
     *
     * @param credential A refreshed Google {@link Credential} object.
     * @return A fully initialized {@link GamGeoResponse}
     * @throws GamException Indicates an Exception has been thrown while attempting to fetch Gam Geo data.
     */
    private GamGeoResponse getZipCodes(Credential oAuth2Credential) throws GamException {

        try {
            // Initiate a new session
            AdManagerSession session = adManagerServices.initAdManagerSession(oAuth2Credential);

            // To get geo data(Zip Codes) using pql interface
            PublisherQueryLanguageServiceInterface geoResultSet = adManagerServices.initPqlService(session);

            String ZIP_CODE_TYPE = "Postal_Code";
            ResultSet zipCodeData  = geoResultSet.select(GamUtils.generateGeoStatement(ZIP_CODE_TYPE));

            // Initialize return object
            GamGeoResponse response = new GamGeoResponse(Pql.resultSetToStringArrayList(zipCodeData));

            log.info("Zip Codes downloaded!");

            log.info("Total zip codes before dedupe : {}", response.getZipCodes().size());

            log.info("Total zip codes after dedupe : {}", response.getResponseList().size());

            return response;

        } catch (Exception e) {
            throw new GamException(e.getMessage(), e.getCause());
        }
    }


    @Deprecated
    @DoNotCall
    @Override
    public GamResponse get(GamRequest request, Credential credential) throws GamException {
        return null;
    }

    @Override
    public GamResponse get(Credential credential) throws GamException {

        return getZipCodes(credential);
    }

    @Deprecated
    @DoNotCall
    @Override
    public Credential get() throws GamException {
        return null;
    }
}
