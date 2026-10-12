package com.example.webhookengine.service;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Periodically queries PostgreSQL for webhook events eligible for retry (status RETRY_PENDING,
 * nextRetryAt <= now) and safely claims and re-enqueues them via the Transactional Outbox.
 */
@Component
@ConditionalOnProperty(name = "app.retry.enabled", havingValue = "true", matchIfMissing = true)
public class WebhookRetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(WebhookRetryScheduler.class);

    private final WebhookEventRepository webhookEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private org.springframework.transaction.PlatformTransactionManager transactionManager;

    @Value("${app.retry.batch-size:20}")
    private int batchSize = 20;

    public WebhookRetryScheduler(WebhookEventRepository webhookEventRepository,
                                 OutboxEventRepository outboxEventRepository,
                                 ObjectMapper objectMapper) {
        this.webhookEventRepository = webhookEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Scheduled job polling for due retries.
     */
    @Scheduled(fixedDelayString = "${app.retry.scheduler-interval-ms:1000}")
    public void scheduleDueRetries() {
        Instant now = Instant.now();
        List<WebhookEvent> dueEvents = webhookEventRepository.findDueEventsForRetry(
            WebhookStatus.RETRY_PENDING,
            now,
            PageRequest.of(0, batchSize)
        );

        if (dueEvents.isEmpty()) {
            return;
        }

        log.info("Found {} webhook events eligible for retry", dueEvents.size());

        for (WebhookEvent event : dueEvents) {
            try {
                if (transactionManager != null) {
                    new org.springframework.transaction.support.TransactionTemplate(transactionManager)
                        .execute(status -> claimAndEnqueueRetry(event));
                } else {
                    claimAndEnqueueRetry(event);
                }
            } catch (Exception e) {
                log.error("Failed to claim and re-enqueue retry for webhook event internalId={}: {}",
                    event.getId(), e.getMessage(), e);
            }
        }
    }

    /**
     * Atomically claims an individual due event (atomic UPDATE status RETRY_PENDING -> RECEIVED)
     * and writes a corresponding PENDING OutboxEvent in the same database transaction.
     * <p>
     * If another concurrent scheduler instance has already claimed the event, the claim query returns 0,
     * and no duplicate outbox event is created.
     *
     * @param event the eligible WebhookEvent candidate
     * @return true if claimed and enqueued, false if another instance won the claim race
     */
    @Transactional
    public boolean claimAndEnqueueRetry(WebhookEvent event) {
        Instant now = Instant.now();

        // 1. Atomic claim check-and-set: only update if status is still RETRY_PENDING
        int updated = webhookEventRepository.claimEventForRetry(
            event.getId(),
            WebhookStatus.RETRY_PENDING,
            WebhookStatus.RECEIVED,
            now
        );

        if (updated == 0) {
            log.debug("Event internalId={} was already claimed by another scheduler instance. Skipping.", event.getId());
            return false;
        }

        // 2. Build lightweight WebhookProcessingMessage matching standard broker contract
        WebhookProcessingMessage outboxMessage = new WebhookProcessingMessage(
            event.getId(),
            event.getEventId(),
            event.getSource(),
            event.getEventType()
        );

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(outboxMessage);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox message for retry", e);
        }

        // 3. Persist OutboxEvent in PENDING status within the same transaction
        OutboxEvent outboxEvent = new OutboxEvent(
            event.getId().toString(),
            event.getEventType(),
            payloadJson,
            OutboxStatus.PENDING
        );
        outboxEventRepository.saveAndFlush(outboxEvent);

        log.info("Successfully re-enqueued retry for webhook event: internalId={}, eventId={}, source={}, outboxId={}",
            event.getId(), event.getEventId(), event.getSource(), outboxEvent.getId());

        return true;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
