package com.example.webhookengine.service;

import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class WebhookProcessingService {

    private static final Logger log = LoggerFactory.getLogger(WebhookProcessingService.class);

    private final WebhookEventRepository webhookEventRepository;
    private final WebhookAttemptRepository webhookAttemptRepository;
    private final WebhookEventProcessor webhookEventProcessor;
    private final RetryPolicy retryPolicy;

    public WebhookProcessingService(WebhookEventRepository webhookEventRepository,
                                    WebhookAttemptRepository webhookAttemptRepository,
                                    WebhookEventProcessor webhookEventProcessor,
                                    RetryPolicy retryPolicy) {
        this.webhookEventRepository = webhookEventRepository;
        this.webhookAttemptRepository = webhookAttemptRepository;
        this.webhookEventProcessor = webhookEventProcessor;
        this.retryPolicy = retryPolicy;
    }

    /**
     * Executes asynchronous processing for a webhook event referenced by its internal ID.
     * Guaranteed idempotent across redeliveries and concurrent claims.
     */
    @Transactional
    public void processEventById(UUID webhookEventId) {
        if (webhookEventId == null) {
            log.warn("Received processing request with null webhookEventId. Discarding.");
            return;
        }

        Optional<WebhookEvent> eventOpt = webhookEventRepository.findById(webhookEventId);
        if (eventOpt.isEmpty()) {
            log.warn("Webhook event not found in database for internalId={}. Message will be acknowledged.", webhookEventId);
            return;
        }

        WebhookEvent event = eventOpt.get();

        // 1. Guard against completed events
        if (event.getStatus() == WebhookStatus.COMPLETED) {
            log.info("Webhook event already COMPLETED for internalId={}, eventId={}. Skipping duplicate delivery.",
                event.getId(), event.getEventId());
            return;
        }

        // 2. Guard against concurrent active processing
        if (event.getStatus() == WebhookStatus.PROCESSING) {
            log.info("Webhook event is already in PROCESSING status for internalId={}, eventId={}. Skipping concurrent execution.",
                event.getId(), event.getEventId());
            return;
        }

        // 3. Claim event
        int nextAttemptNumber = event.getAttemptCount() + 1;
        event.setStatus(WebhookStatus.PROCESSING);
        event.setAttemptCount(nextAttemptNumber);
        webhookEventRepository.saveAndFlush(event);

        Instant startedAt = Instant.now();
        WebhookAttempt attempt = new WebhookAttempt(
            event,
            nextAttemptNumber,
            WebhookStatus.PROCESSING
        );
        attempt.setStartedAt(startedAt);

        log.info("Claimed webhook event for processing: internalId={}, eventId={}, attemptNumber={}",
            event.getId(), event.getEventId(), nextAttemptNumber);

        // 4. Execute business logic
        try {
            webhookEventProcessor.processEvent(event);

            // 5. Update to COMPLETED on success
            Instant completedAt = Instant.now();
            event.setStatus(WebhookStatus.COMPLETED);
            event.setProcessedAt(completedAt);
            event.setLastError(null);
            event.setNextRetryAt(null);
            webhookEventRepository.saveAndFlush(event);

            attempt.setStatus(WebhookStatus.COMPLETED);
            attempt.setCompletedAt(completedAt);
            attempt.setErrorMessage(null);
            webhookAttemptRepository.saveAndFlush(attempt);

            log.info("Successfully completed processing for webhook event: internalId={}, eventId={}, attemptNumber={}",
                event.getId(), event.getEventId(), nextAttemptNumber);
        } catch (Exception ex) {
            Instant failedAt = Instant.now();
            String errorMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();

            log.error("Failed processing webhook event: internalId={}, eventId={}, attemptNumber={}, error={}",
                event.getId(), event.getEventId(), nextAttemptNumber, errorMsg, ex);

            // Record failed attempt
            attempt.setStatus(WebhookStatus.FAILED);
            attempt.setCompletedAt(failedAt);
            attempt.setErrorMessage(errorMsg);
            webhookAttemptRepository.saveAndFlush(attempt);

            event.setLastError(errorMsg);

            // Determine retry eligibility using exponential backoff policy
            if (retryPolicy.canRetry(nextAttemptNumber)) {
                Instant nextRetry = retryPolicy.calculateNextRetryAt(nextAttemptNumber, failedAt);
                event.setStatus(WebhookStatus.RETRY_PENDING);
                event.setNextRetryAt(nextRetry);
                log.info("Scheduled retry for webhook event: internalId={}, eventId={}, failedAttempt={}, nextRetryAt={}",
                    event.getId(), event.getEventId(), nextAttemptNumber, nextRetry);
            } else {
                event.setStatus(WebhookStatus.FAILED);
                event.setNextRetryAt(null);
                log.warn("Exhausted maximum retry attempts ({}) for webhook event: internalId={}, eventId={}. Marked as FAILED.",
                    retryPolicy.getProperties().getMaxAttempts(), event.getId(), event.getEventId());
            }

            webhookEventRepository.saveAndFlush(event);

            throw ex; // Re-throw to allow listener / error handler to handle message acknowledgment
        }
    }
}
