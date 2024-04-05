package com.target.kelsaapi.common.service.authentication;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.Gson;
import com.target.kelsaapi.common.constants.ApplicationConstants;
import com.target.kelsaapi.common.exceptions.ConfigurationException;
import com.target.kelsaapi.pipelines.config.authentication.Oauth2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class  having various types of authentication mechanisms
 */
@Slf4j
@Service("authenticationService")
public class AuthenticationServiceImpl implements AuthenticationService {

    @Autowired
    @Qualifier("restTemplate")
    RestTemplate restTemplate;

    @Override
    public String getOauth2Token(String oauthUrl, HttpEntity<?> requestEntity) {
        return getOauth2Token(oauthUrl, requestEntity, ApplicationConstants.ACCESSTOKEN, true);
    }
    @Override
    public String getOauth2Token(String oauthUrl, HttpEntity<?> requestEntity, String accessTokenName, Boolean prependBearerName) {
        log.info("Performing authentication with oauth2");
        String oauthToken;
        ResponseEntity<JsonNode> responseEntity = restTemplate.exchange(oauthUrl,
                HttpMethod.POST,
                requestEntity,
                JsonNode.class);
        String bearer;
        try {
            log.debug("Response from Oauth request::{}", responseEntity.getBody().toPrettyString());
            log.debug("Attempting to retrieve access token with key name::{}", accessTokenName);
            if (accessTokenName.contains("/")) {
                String prependAccessTokenName;
                if (!accessTokenName.startsWith("/")) prependAccessTokenName = "/" + accessTokenName; else prependAccessTokenName = accessTokenName;
                JsonNode jsonNode = responseEntity.getBody();
                bearer = jsonNode.at(prependAccessTokenName).asText();
            } else {
                bearer = responseEntity.getBody().get(accessTokenName).asText();
            }
        } catch (NullPointerException e) {
            log.error("No valid response while getting oAuth token");
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        if (prependBearerName) {
            oauthToken = ApplicationConstants.BEARER + bearer;
        } else {
            oauthToken = bearer;
        }

        log.debug("Bearer token is {}", oauthToken);
        return oauthToken;
    }

    @Override
    public HttpEntity<?> getOauth2RequestObject(Oauth2 oauth2) throws ConfigurationException {
        MultiValueMap<String, String> headerMap = new LinkedMultiValueMap<>();
        MultiValueMap<String, String> bodyMap = new LinkedMultiValueMap<>();
        Gson gson = new Gson();
        String contentType = "application/x-www-form-urlencoded";
        if (oauth2.getHeadersMap() != null) {
            for (Map.Entry<String, String> entry : oauth2.getHeadersMap().entrySet()) {
                log.debug("Adding header to request object:: key: {}, value: {}", entry.getKey(), entry.getValue());
                headerMap.add(entry.getKey(), entry.getValue());
                if (Objects.equals(entry.getKey(), "Content-Type")) {
                    contentType = entry.getValue();
                }
            }
        } else {
            throw new ConfigurationException("Missing headersMap from Oauth2 configuration");
        }
        if (oauth2.getBodyMap() != null) {
            Map<String, String> configMap = oauth2.getBodyMap();

            if(oauth2.oauthUrl.contains("appnexus")) {

                Map<String, Map<String, String>> configMap2 = new HashMap<>();
                Map<String, String> creds = new HashMap<String, String>();
                creds.put("username", configMap.get("username"));
                creds.put("password", configMap.get("password"));
                configMap2.put("auth", creds);
                String gsonBody = gson.toJson(configMap2);
                log.debug("Adding body to request object as json:: {}", gsonBody);
                return new HttpEntity<>(gsonBody, headerMap);
            }

            if (Objects.equals(contentType, "application/json")) {

                String gsonBody = gson.toJson(configMap);
                log.debug("Adding body to request object as json:: {}", gsonBody);
                return new HttpEntity<>(gsonBody, headerMap);
            } else {
                for (Map.Entry<String, String> entry : configMap.entrySet()) {
                    log.debug("Adding body to request object:: key: {}, value: {}", entry.getKey(), entry.getValue());
                    bodyMap.add(entry.getKey(), String.valueOf(entry.getValue()));
                }
                return new HttpEntity<>(bodyMap, headerMap);
            }

        }
        log.info("No bodyMap from Oauth2 configuration, sending only headers in request.");
        return new HttpEntity<>(null, headerMap);
    }
}
