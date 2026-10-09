package com.example.webhookengine.service;

import com.example.webhookengine.controller.dto.WebhookIngestRequest;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.messaging.WebhookProcessingMessage;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class WebhookIngestionService {

    private static final Logger log = LoggerFactory.getLogger(WebhookIngestionService.class);

    private final WebhookEventRepository webhookEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public WebhookIngestionService(WebhookEventRepository webhookEventRepository,
                                   OutboxEventRepository outboxEventRepository,
                                   ObjectMapper objectMapper) {
        this.webhookEventRepository = webhookEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Ingests an incoming webhook event idempotently using the Transactional Outbox Pattern.
     * <p>
     * 1. Checks if an event with (source, eventId) already exists. If yes, returns it as existing.
     * 2. If not, attempts to persist WebhookEvent (status: RECEIVED) and OutboxEvent (status: PENDING)
     *    in the same atomic database transaction.
     * 3. In the event of a concurrent race condition violating unique(source, event_id), catches
     *    the DataIntegrityViolationException and retrieves the concurrently inserted record.
     * </p>
     */
    public IngestionResult ingestWebhook(WebhookIngestRequest request) {
        String source = request.getSource();
        String eventId = request.getEventId();

        // 1. Check existing record
        Optional<WebhookEvent> existing = webhookEventRepository.findBySourceAndEventId(source, eventId);
        if (existing.isPresent()) {
            log.info("Duplicate webhook event detected for source: {} and eventId: {}", source, eventId);
            return new IngestionResult(existing.get(), false);
        }

        // 2. Serialize payload to JSON string
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(request.getPayload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize payload to JSON string", e);
        }

        WebhookEvent newEvent = new WebhookEvent(
            eventId,
            source,
            request.getEventType(),
            payloadJson,
            WebhookStatus.RECEIVED
        );

        // 3. Persist WebhookEvent and OutboxEvent atomically within the same transaction
        try {
            WebhookEvent saved = persistEventAndOutbox(newEvent);
            log.info("Successfully ingested new webhook event with id: {}, source: {}, eventId: {} and created OutboxEvent",
                saved.getId(), source, eventId);

            return new IngestionResult(saved, true);
        } catch (DataIntegrityViolationException e) {
            log.warn("Concurrent duplicate detected for source: {} and eventId: {}. Fetching existing record.",
                source, eventId);
            return webhookEventRepository.findBySourceAndEventId(source, eventId)
                .map(event -> new IngestionResult(event, false))
                .orElseThrow(() -> e); // If not due to (source, event_id) duplicate, rethrow
        }
    }

    @Transactional
    public WebhookEvent persistEventAndOutbox(WebhookEvent event) {
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        // Build lightweight Outbox processing payload matching WebhookProcessingMessage contract
        WebhookProcessingMessage outboxMessage = new WebhookProcessingMessage(
            savedEvent.getId(),
            savedEvent.getEventId(),
            savedEvent.getSource(),
            savedEvent.getEventType()
        );

        String outboxPayloadJson;
        try {
            outboxPayloadJson = objectMapper.writeValueAsString(outboxMessage);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OutboxProcessingMessage", e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
            savedEvent.getId().toString(),
            savedEvent.getEventType(),
            outboxPayloadJson,
            OutboxStatus.PENDING
        );

        outboxEventRepository.saveAndFlush(outboxEvent);
        return savedEvent;
    }
}
