package com.example.webhookengine.service;

import com.example.webhookengine.controller.dto.DeadLetterDetailResponse;
import com.example.webhookengine.controller.dto.DeadLetterSummaryResponse;
import com.example.webhookengine.controller.dto.WebhookAttemptResponse;
import com.example.webhookengine.controller.dto.WebhookReplayResponse;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookReplay;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.InvalidStateTransitionException;
import com.example.webhookengine.exception.ResourceNotFoundException;
import com.example.webhookengine.messaging.WebhookProcessingMessage;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.repository.WebhookReplayRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final WebhookEventRepository webhookEventRepository;
    private final WebhookAttemptRepository webhookAttemptRepository;
    private final WebhookReplayRepository webhookReplayRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public DeadLetterService(WebhookEventRepository webhookEventRepository,
                             WebhookAttemptRepository webhookAttemptRepository,
                             WebhookReplayRepository webhookReplayRepository,
                             OutboxEventRepository outboxEventRepository,
                             ObjectMapper objectMapper) {
        this.webhookEventRepository = webhookEventRepository;
        this.webhookAttemptRepository = webhookAttemptRepository;
        this.webhookReplayRepository = webhookReplayRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Lists all events currently in DEAD_LETTER status with pagination support.
     */
    @Transactional(readOnly = true)
    public Page<DeadLetterSummaryResponse> listDeadLetters(Pageable pageable) {
        return webhookEventRepository.findByStatus(WebhookStatus.DEAD_LETTER, pageable)
            .map(this::toSummaryResponse);
    }

    /**
     * Inspects a specific event by internal UUID.
     * Throws ResourceNotFoundException (404) if not found.
     * Throws InvalidStateTransitionException (409) if the event exists but is not in DEAD_LETTER status.
     */
    @Transactional(readOnly = true)
    public DeadLetterDetailResponse inspectDeadLetter(UUID id) {
        WebhookEvent event = webhookEventRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Webhook event not found with ID: " + id));

        if (event.getStatus() != WebhookStatus.DEAD_LETTER) {
            throw new InvalidStateTransitionException(
                String.format("Webhook event with ID %s is not in DEAD_LETTER state (current status: %s)", id, event.getStatus())
            );
        }

        List<WebhookAttemptResponse> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(event)
            .stream()
            .map(this::toAttemptResponse)
            .toList();

        List<WebhookReplayResponse> replays = webhookReplayRepository.findByWebhookEventOrderByRequestedAtDesc(event)
            .stream()
            .map(this::toReplayResponse)
            .toList();

        return new DeadLetterDetailResponse(
            event.getId(),
            event.getEventId(),
            event.getSource(),
            event.getEventType(),
            event.getStatus(),
            event.getAttemptCount(),
            event.getLastError(),
            event.getCreatedAt(),
            event.getUpdatedAt(),
            event.getNextRetryAt(),
            attempts,
            replays
        );
    }

    /**
     * Safely replays a dead-letter event within a single database transaction.
     * Uses atomic DB conditional update (claimEventForReplay) to prevent concurrent replay races.
     * Transitions event to RECEIVED, clears nextRetryAt, preserves attemptCount and historical attempts,
     * persists replay audit record, and creates an OutboxEvent (PENDING) containing WebhookProcessingMessage.
     */
    @Transactional
    public WebhookReplayResponsePayload replayDeadLetter(UUID id, String reason) {
        WebhookEvent event = webhookEventRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Webhook event not found with ID: " + id));

        if (event.getStatus() != WebhookStatus.DEAD_LETTER) {
            throw new InvalidStateTransitionException(
                String.format("Webhook event with ID %s is not eligible for replay (current status: %s, required: DEAD_LETTER)", id, event.getStatus())
            );
        }

        Instant now = Instant.now();

        // 1. Atomic claim: UPDATE status DEAD_LETTER -> RECEIVED where id = :id AND status = 'DEAD_LETTER'
        int claimed = webhookEventRepository.claimEventForReplay(
            id,
            WebhookStatus.DEAD_LETTER,
            WebhookStatus.RECEIVED,
            now
        );

        if (claimed == 0) {
            log.warn("Concurrent replay race lost or event status modified for internalId={}", id);
            throw new InvalidStateTransitionException(
                String.format("Webhook event with ID %s could not be claimed for replay (possibly concurrent replay request)", id)
            );
        }

        // Refresh entity in persistence context
        WebhookEvent refreshedEvent = webhookEventRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Webhook event not found with ID: " + id));

        // 2. Record replay audit
        WebhookReplay replayAudit = new WebhookReplay(
            refreshedEvent,
            reason,
            refreshedEvent.getAttemptCount()
        );
        webhookReplayRepository.saveAndFlush(replayAudit);

        // 3. Create transactional OutboxEvent
        WebhookProcessingMessage outboxMessage = new WebhookProcessingMessage(
            refreshedEvent.getId(),
            refreshedEvent.getEventId(),
            refreshedEvent.getSource(),
            refreshedEvent.getEventType()
        );

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(outboxMessage);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox message for dead-letter replay", e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
            refreshedEvent.getId().toString(),
            refreshedEvent.getEventType(),
            payloadJson,
            OutboxStatus.PENDING
        );
        outboxEventRepository.saveAndFlush(outboxEvent);

        log.info("Successfully replayed dead-letter event: internalId={}, eventId={}, source={}, outboxId={}",
            refreshedEvent.getId(), refreshedEvent.getEventId(), refreshedEvent.getSource(), outboxEvent.getId());

        return new WebhookReplayResponsePayload(
            refreshedEvent.getId(),
            refreshedEvent.getEventId(),
            refreshedEvent.getSource(),
            refreshedEvent.getStatus(),
            "Webhook event replay accepted and enqueued for processing"
        );
    }

    private DeadLetterSummaryResponse toSummaryResponse(WebhookEvent event) {
        return new DeadLetterSummaryResponse(
            event.getId(),
            event.getEventId(),
            event.getSource(),
            event.getEventType(),
            event.getStatus(),
            event.getAttemptCount(),
            event.getLastError(),
            event.getCreatedAt(),
            event.getUpdatedAt()
        );
    }

    private WebhookAttemptResponse toAttemptResponse(WebhookAttempt attempt) {
        return new WebhookAttemptResponse(
            attempt.getId(),
            attempt.getAttemptNumber(),
            attempt.getStatus(),
            attempt.getStartedAt(),
            attempt.getCompletedAt(),
            attempt.getErrorMessage()
        );
    }

    private WebhookReplayResponse toReplayResponse(WebhookReplay replay) {
        return new WebhookReplayResponse(
            replay.getId(),
            replay.getRequestedAt(),
            replay.getReason(),
            replay.getPreviousAttemptCount()
        );
    }
}
