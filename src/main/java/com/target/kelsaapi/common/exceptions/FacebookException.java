package com.target.kelsaapi.common.exceptions;

/**
 * Exception used by the FacebookService implementations
 */
public class FacebookException extends RuntimeException {

    public FacebookException(String message) {
        super(message);
    }

    public FacebookException(String message, Throwable cause) {
        super(message, cause);
    }

    public FacebookException(Throwable cause) {
        super(cause);
    }
}
