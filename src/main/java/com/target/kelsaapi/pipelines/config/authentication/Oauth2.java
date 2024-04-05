package com.target.kelsaapi.pipelines.config.authentication;

import lombok.Data;
import org.springframework.lang.Nullable;

import java.util.Map;

@Data
public class Oauth2 {

    public String oauthUrl;

    @Nullable
    public Map<String, String> bodyMap;

    @Nullable
    private String accessToken;

    @Nullable
    public Map<String, String> headersMap;

    @Nullable
    public String responseTokenKeyName;

}
