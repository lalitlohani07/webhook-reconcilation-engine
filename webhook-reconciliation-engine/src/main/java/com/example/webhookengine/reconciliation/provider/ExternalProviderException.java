package com.example.webhookengine.reconciliation.provider;

public class ExternalProviderException extends Exception {

    public ExternalProviderException(String message) {
        super(message);
    }

    public ExternalProviderException(String message, Throwable cause) {
        super(message, cause);
    }
}
