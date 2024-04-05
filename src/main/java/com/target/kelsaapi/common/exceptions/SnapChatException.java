package com.target.kelsaapi.common.exceptions;

public class SnapChatException extends Exception {

    public SnapChatException(String message) {
        super(message);
    }

    public SnapChatException(String message, Throwable cause) {
        super(message, cause);
    }

    public SnapChatException(Throwable cause) {
        super(cause);
    }
}