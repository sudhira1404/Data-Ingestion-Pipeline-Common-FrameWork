package com.target.kelsaapi.common.exceptions;


/**
 * Exception used for writing/sink Errors
 *
 * @since 1.0
 */

public class ReaderException extends Exception {

    public ReaderException(String message) {
        super(message);
    }

    public ReaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReaderException(Throwable cause) {
        super(cause);
    }

}
