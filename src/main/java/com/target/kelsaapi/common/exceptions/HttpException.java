package com.target.kelsaapi.common.exceptions;

import java.io.IOException;

/**
 * Exception used for Authentication Errors
 *
 * @since 1.0
 */
public class HttpException extends IOException {

    public HttpException(String message) {
        super(message);
    }

    public HttpException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpException(Throwable cause) {
        super(cause);
    }

}
