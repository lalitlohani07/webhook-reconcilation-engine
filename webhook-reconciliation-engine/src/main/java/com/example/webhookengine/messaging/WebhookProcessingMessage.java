package com.example.webhookengine.messaging;

import java.util.UUID;

/**
 * Lightweight DTO for publishing webhook processing jobs to RabbitMQ.
 * Avoids passing JPA entities, Hibernate proxies, or large payloads over the broker.
 */
public record WebhookProcessingMessage(
    UUID webhookEventId,
    String eventId,
    String source,
    String eventType
) {
    public WebhookProcessingMessage {
        if (webhookEventId == null) {
            throw new IllegalArgumentException("webhookEventId cannot be null");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId cannot be null or blank");
        }
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source cannot be null or blank");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType cannot be null or blank");
        }
    }
}
