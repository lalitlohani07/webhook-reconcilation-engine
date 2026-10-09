package com.example.webhookengine.messaging;

import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
@ConditionalOnProperty(name = "app.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final WebhookEventPublisher webhookEventPublisher;
    private final ObjectMapper objectMapper;

    @Value("${app.outbox.batch-size:10}")
    private int batchSize = 10;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           WebhookEventPublisher webhookEventPublisher,
                           ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.webhookEventPublisher = webhookEventPublisher;
        this.objectMapper = objectMapper;
    }

    /**
     * Periodically queries pending outbox events and publishes them to RabbitMQ.
     * Transaction boundaries are kept short: fetching/claiming and updating status are transactional,
     * while broker network calls occur outside the database transaction.
     */
    @Scheduled(fixedDelayString = "${app.outbox.polling-interval-ms:2000}")
    public void processOutboxEvents() {
        List<OutboxEvent> pendingEvents = fetchPendingEvents();
        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Discovered {} pending outbox records to publish", pendingEvents.size());

        for (OutboxEvent outboxEvent : pendingEvents) {
            publishSingleEvent(outboxEvent);
        }
    }

    public List<OutboxEvent> fetchPendingEvents() {
        return outboxEventRepository.findPendingEventsForProcessing(OutboxStatus.PENDING, PageRequest.of(0, batchSize));
    }

    public void publishSingleEvent(OutboxEvent outboxEvent) {
        log.info("Publication started for outboxId={}, aggregateId={}, eventType={}",
            outboxEvent.getId(), outboxEvent.getAggregateId(), outboxEvent.getEventType());

        WebhookProcessingMessage message;
        try {
            message = objectMapper.readValue(outboxEvent.getPayload(), WebhookProcessingMessage.class);
        } catch (Exception e) {
            log.error("Failed to deserialize outbox payload for outboxId={}: {}", outboxEvent.getId(), e.getMessage());
            markEventFailed(outboxEvent.getId(), "Payload deserialization failed: " + e.getMessage());
            return;
        }

        try {
            // Broker network call with publisher confirmation check
            webhookEventPublisher.publishProcessingMessage(message);

            // Confirmed publication: transition to PUBLISHED
            markEventPublished(outboxEvent.getId());
            log.info("Publication confirmed and record marked PUBLISHED for outboxId={}, aggregateId={}",
                outboxEvent.getId(), outboxEvent.getAggregateId());
        } catch (Exception e) {
            log.warn("Publication failed for outboxId={}, aggregateId={}: {}. Record left PENDING for retry.",
                outboxEvent.getId(), outboxEvent.getAggregateId(), e.getMessage());
            recordPublicationAttemptFailure(outboxEvent.getId(), e.getMessage());
        }
    }

    @Transactional
    public void markEventPublished(java.util.UUID outboxId) {
        outboxEventRepository.findById(outboxId).ifPresent(event -> {
            event.setStatus(OutboxStatus.PUBLISHED);
            event.setPublishedAt(Instant.now());
            event.setLastError(null);
            outboxEventRepository.saveAndFlush(event);
        });
    }

    @Transactional
    public void recordPublicationAttemptFailure(java.util.UUID outboxId, String errorMessage) {
        outboxEventRepository.findById(outboxId).ifPresent(event -> {
            event.setAttemptCount(event.getAttemptCount() + 1);
            event.setLastError(errorMessage);
            // Notice: Status remains PENDING so it is eligible for subsequent polling recovery
            outboxEventRepository.saveAndFlush(event);
        });
    }

    @Transactional
    public void markEventFailed(java.util.UUID outboxId, String errorMessage) {
        outboxEventRepository.findById(outboxId).ifPresent(event -> {
            event.setStatus(OutboxStatus.FAILED);
            event.setLastError(errorMessage);
            outboxEventRepository.saveAndFlush(event);
        });
    }
}
