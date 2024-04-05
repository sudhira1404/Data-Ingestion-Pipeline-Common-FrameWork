package com.target.kelsaapi.common.exceptions;

/**
 * Exception used for Configurations Errors
 *
 * @since 1.0
 */
public class ConfigurationException extends Exception {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }

}
