package com.target.kelsaapi.common.service.authentication;

import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.pipelines.config.authentication.Oauth2;
import org.springframework.http.HttpEntity;

/**
 * Interface having all the authentication mechanisms
 *
 * @since 1.0
 */
public interface AuthenticationService {

    String getOauth2Token(String oauthUrl, HttpEntity<?> requestEntity);
    HttpEntity<?> getOauth2RequestObject(Oauth2 oauth2) throws ConfigurationException;
    String getOauth2Token(String oauthUrl, HttpEntity<?> requestEntity, String accessTokenName, Boolean prependBearerName);
}
