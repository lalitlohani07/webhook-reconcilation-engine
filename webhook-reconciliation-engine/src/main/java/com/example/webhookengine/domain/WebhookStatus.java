package com.example.webhookengine.domain;

public enum WebhookStatus {
    RECEIVED,
    PROCESSING,
    COMPLETED,
    RETRY_PENDING,
    FAILED,
    DEAD_LETTER
}
