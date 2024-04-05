package com.target.kelsaapi.common.exceptions;

import java.io.IOException;

/**
 * Exception used for Authentication Errors
 *
 * @since 1.0
 */
public class HttpRetryableException extends IOException {

    public HttpRetryableException(String message) {
        super(message);
    }

    public HttpRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpRetryableException(Throwable cause) {
        super(cause);
    }

}
