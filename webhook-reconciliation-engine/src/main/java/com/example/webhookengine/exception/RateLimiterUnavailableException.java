package com.example.webhookengine.exception;

public class RateLimiterUnavailableException extends RuntimeException {

    public RateLimiterUnavailableException(String message) {
        super(message);
    }

    public RateLimiterUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
