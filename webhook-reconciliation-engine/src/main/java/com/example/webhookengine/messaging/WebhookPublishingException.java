package com.example.webhookengine.messaging;

public class WebhookPublishingException extends RuntimeException {

    public WebhookPublishingException(String message) {
        super(message);
    }

    public WebhookPublishingException(String message, Throwable cause) {
        super(message, cause);
    }
}
