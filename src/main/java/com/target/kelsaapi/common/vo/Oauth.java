package com.target.kelsaapi.common.vo;

import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.common.service.authentication.AuthenticationService;
import com.target.kelsaapi.pipelines.config.authentication.Oauth2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;

@Slf4j
public class Oauth {
    @Getter
    private String oAuthToken;

    private final AuthenticationService auth;

    private final Oauth2 config;

    public Oauth(ApplicationContext context, Oauth2 config) throws ConfigurationException {
        this.config = config;
        this.auth = context.getBean(AuthenticationService.class);
        setOAuthToken();
    }

    private void setOAuthToken() throws ConfigurationException {
        String accessToken;
        if (config.getAccessToken() != null) {
            log.info("Using static access token for auth");
            log.debug("Static access token: " + config.getAccessToken());
            accessToken = config.getAccessToken();
        } else {
            log.debug("Fetching short term token from Oauth URL: " + config.getOauthUrl());
            if (config.getHeadersMap() != null) {
                log.debug("Oauth request headers: " + config.getHeadersMap().toString());
            }
            if (config.getBodyMap() != null) {
                log.debug("Oauth request body: " + config.getBodyMap().toString());
            }
            HttpEntity<?> requestAuth = auth.getOauth2RequestObject(config);
            if (config.responseTokenKeyName == null) {
                accessToken = auth.getOauth2Token(config.getOauthUrl(), requestAuth);
            } else {
                accessToken = auth.getOauth2Token(config.getOauthUrl(), requestAuth, config.getResponseTokenKeyName(), false);
            }
        }
        this.oAuthToken = accessToken;
    }
}
