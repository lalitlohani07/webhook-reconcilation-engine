package com.example.webhookengine.service;

import com.example.webhookengine.domain.WebhookEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Minimal deterministic event processor representing domain business execution.
 * Validates payload presence and event metadata. Easily pluggable for real integrations in later phases.
 */
@Service
public class WebhookEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventProcessor.class);

    public void processEvent(WebhookEvent event) {
        log.info("Processing webhook event: internalId={}, eventId={}, source={}, eventType={}",
            event.getId(), event.getEventId(), event.getSource(), event.getEventType());

        // Basic deterministic processing logic
        if (event.getPayload() == null || event.getPayload().isBlank()) {
            throw new IllegalArgumentException("Cannot process event with null or empty payload: " + event.getId());
        }

        log.info("Successfully executed business logic for event: internalId={}, eventId={}",
            event.getId(), event.getEventId());
    }
}
