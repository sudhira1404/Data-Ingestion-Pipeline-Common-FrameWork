package com.target.kelsaapi.common.exceptions;

/**
 * Exception used for writing/sink Errors
 *
 * @since 1.0
 */
public class WriterException extends Exception {

    public WriterException(String message) {
        super(message);
    }

    public WriterException(String message, Throwable cause) {
        super(message, cause);
    }

    public WriterException(Throwable cause) {
        super(cause);
    }

}
