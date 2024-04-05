package com.target.kelsaapi.common.exceptions;

/**
 * Exception used for Pagination Errors
 *
 * @since 1.0
 */
public class PaginationException extends Exception {

    public PaginationException(String message) {
        super(message);
    }

    public PaginationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PaginationException(Throwable cause) {
        super(cause);
    }

}