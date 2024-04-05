package com.target.kelsaapi.common.vo;

import lombok.Data;
import org.springframework.http.HttpHeaders;

/**
 * Response of HTTP code
 */
@Data
public class HttpCustomResponse {
    final HttpHeaders headers;
    final String body;
    final Long statusCode;
}
