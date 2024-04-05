package com.target.kelsaapi.common.exceptions;

/**
 * Exception used for if the functionality is yet to be implemented or not supported
 *
 * @since 1.0
 */
public class NotSupportedException extends Exception {

    public NotSupportedException(String message) {
        super(message);
    }

    public NotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotSupportedException(Throwable cause) {
        super(cause);
    }

}
