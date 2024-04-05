package com.target.kelsaapi.common.exceptions;

/**
 * Exception used for Authentication Errors
 *
 * @since 1.0
 */
public class UnImplementedException extends Exception {

    public UnImplementedException(String message) {
        super(message);
    }

    public UnImplementedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnImplementedException(Throwable cause) {
        super(cause);
    }

}
