package com.target.kelsaapi.common.exceptions;

/**
 * Exception used for Authentication Errors
 *
 * @since 1.0
 */
public class AuthenticationException extends Exception {

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthenticationException(Throwable cause) {
        super(cause);
    }

}
