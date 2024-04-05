package com.target.kelsaapi.common.exceptions;

public class GamException extends Exception {

    public GamException(String message) {
        super(message);
    }

    public GamException(String message, Throwable cause) {
        super(message, cause);
    }

    public GamException(Throwable cause) {
        super(cause);
    }
}
