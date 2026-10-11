package com.example.webhookengine.service;

import com.example.webhookengine.controller.dto.DeadLetterDetailResponse;
import com.example.webhookengine.controller.dto.DeadLetterSummaryResponse;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookReplay;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.InvalidStateTransitionException;
import com.example.webhookengine.exception.ResourceNotFoundException;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.repository.WebhookReplayRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeadLetterServiceTest {

    @Mock
    private WebhookEventRepository webhookEventRepository;

    @Mock
    private WebhookAttemptRepository webhookAttemptRepository;

    @Mock
    private WebhookReplayRepository webhookReplayRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private ObjectMapper objectMapper;
    private DeadLetterService deadLetterService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        deadLetterService = new DeadLetterService(
            webhookEventRepository,
            webhookAttemptRepository,
            webhookReplayRepository,
            outboxEventRepository,
            objectMapper
        );
    }

    @Test
    @DisplayName("Lists only dead-letter events with pagination")
    void shouldListDeadLettersWithPagination() {
        Pageable pageable = PageRequest.of(0, 10);
        WebhookEvent event1 = new WebhookEvent(UUID.randomUUID(), "evt_1", "stripe", "charge.failed", "{}", WebhookStatus.DEAD_LETTER);
        event1.setAttemptCount(5);
        event1.setLastError("Connection timed out");

        Page<WebhookEvent> page = new PageImpl<>(List.of(event1), pageable, 1);
        when(webhookEventRepository.findByStatus(WebhookStatus.DEAD_LETTER, pageable)).thenReturn(page);

        Page<DeadLetterSummaryResponse> result = deadLetterService.listDeadLetters(pageable);

        assertThat(result.getTotalElements()).isEqualTo(1);
        DeadLetterSummaryResponse summary = result.getContent().get(0);
        assertThat(summary.id()).isEqualTo(event1.getId());
        assertThat(summary.eventId()).isEqualTo("evt_1");
        assertThat(summary.source()).isEqualTo("stripe");
        assertThat(summary.status()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(summary.attemptCount()).isEqualTo(5);
        assertThat(summary.lastError()).isEqualTo("Connection timed out");
    }

    @Test
    @DisplayName("Inspects dead-letter event and returns attempts and replay history")
    void shouldInspectDeadLetterEventSuccessfully() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.failed", "{}", WebhookStatus.DEAD_LETTER);
        event.setAttemptCount(3);
        event.setLastError("Fatal parsing error");

        WebhookAttempt attempt1 = new WebhookAttempt(UUID.randomUUID(), event, 1, WebhookStatus.FAILED, Instant.now().minusSeconds(60));
        attempt1.setErrorMessage("Attempt 1 failed");
        WebhookAttempt attempt2 = new WebhookAttempt(UUID.randomUUID(), event, 2, WebhookStatus.FAILED, Instant.now().minusSeconds(30));
        attempt2.setErrorMessage("Attempt 2 failed");

        WebhookReplay replay = new WebhookReplay(UUID.randomUUID(), event, Instant.now().minusSeconds(10), "Manual test replay", 2);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));
        when(webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(event)).thenReturn(List.of(attempt1, attempt2));
        when(webhookReplayRepository.findByWebhookEventOrderByRequestedAtDesc(event)).thenReturn(List.of(replay));

        DeadLetterDetailResponse detail = deadLetterService.inspectDeadLetter(eventId);

        assertThat(detail.id()).isEqualTo(eventId);
        assertThat(detail.eventId()).isEqualTo("evt_1");
        assertThat(detail.status()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(detail.attemptCount()).isEqualTo(3);
        assertThat(detail.lastError()).isEqualTo("Fatal parsing error");
        assertThat(detail.attempts()).hasSize(2);
        assertThat(detail.attempts().get(0).attemptNumber()).isEqualTo(1);
        assertThat(detail.attempts().get(1).attemptNumber()).isEqualTo(2);
        assertThat(detail.replays()).hasSize(1);
        assertThat(detail.replays().get(0).reason()).isEqualTo("Manual test replay");
    }

    @Test
    @DisplayName("Inspect throws ResourceNotFoundException (404) when event does not exist")
    void shouldThrow404WhenInspectingNonExistentEvent() {
        UUID missingId = UUID.randomUUID();
        when(webhookEventRepository.findById(missingId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deadLetterService.inspectDeadLetter(missingId))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("not found");
    }

    @Test
    @DisplayName("Inspect throws InvalidStateTransitionException (409) when event is not in DEAD_LETTER status")
    void shouldThrow409WhenInspectingNonDeadLetterEvent() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.success", "{}", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));

        assertThatThrownBy(() -> deadLetterService.inspectDeadLetter(eventId))
            .isInstanceOf(InvalidStateTransitionException.class)
            .hasMessageContaining("not in DEAD_LETTER state");
    }

    @Test
    @DisplayName("Replay successfully transitions event to RECEIVED, creates OutboxEvent, and records audit")
    void shouldReplayDeadLetterSuccessfully() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent deadEvent = new WebhookEvent(eventId, "evt_1", "stripe", "payment.failed", "{\"amount\":100}", WebhookStatus.DEAD_LETTER);
        deadEvent.setAttemptCount(5);
        deadEvent.setLastError("Exhausted retries");

        WebhookEvent refreshedEvent = new WebhookEvent(eventId, "evt_1", "stripe", "payment.failed", "{\"amount\":100}", WebhookStatus.RECEIVED);
        refreshedEvent.setAttemptCount(5);
        refreshedEvent.setLastError("Exhausted retries");

        when(webhookEventRepository.findById(eventId))
            .thenReturn(Optional.of(deadEvent))
            .thenReturn(Optional.of(refreshedEvent));

        when(webhookEventRepository.claimEventForReplay(eq(eventId), eq(WebhookStatus.DEAD_LETTER), eq(WebhookStatus.RECEIVED), any(Instant.class)))
            .thenReturn(1);

        WebhookReplayResponsePayload result = deadLetterService.replayDeadLetter(eventId, "Fix deployed");

        assertThat(result.id()).isEqualTo(eventId);
        assertThat(result.eventId()).isEqualTo("evt_1");
        assertThat(result.source()).isEqualTo("stripe");
        assertThat(result.status()).isEqualTo(WebhookStatus.RECEIVED);
        assertThat(result.message()).contains("accepted");

        // Verify outbox event creation
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).saveAndFlush(outboxCaptor.capture());
        OutboxEvent outbox = outboxCaptor.getValue();
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.getAggregateId()).isEqualTo(eventId.toString());
        assertThat(outbox.getPayload()).contains("evt_1");

        // Verify replay audit creation
        ArgumentCaptor<WebhookReplay> replayCaptor = ArgumentCaptor.forClass(WebhookReplay.class);
        verify(webhookReplayRepository).saveAndFlush(replayCaptor.capture());
        WebhookReplay replay = replayCaptor.getValue();
        assertThat(replay.getReason()).isEqualTo("Fix deployed");
        assertThat(replay.getPreviousAttemptCount()).isEqualTo(5);
    }

    @Test
    @DisplayName("Replay throws 404 when target event does not exist")
    void shouldThrow404WhenReplayingNonExistentEvent() {
        UUID missingId = UUID.randomUUID();
        when(webhookEventRepository.findById(missingId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deadLetterService.replayDeadLetter(missingId, "Reason"))
            .isInstanceOf(ResourceNotFoundException.class);

        verify(outboxEventRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Replay throws 409 when target event is not in DEAD_LETTER status")
    void shouldThrow409WhenReplayingNonDeadLetterEvent() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "payment.failed", "{}", WebhookStatus.PROCESSING);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));

        assertThatThrownBy(() -> deadLetterService.replayDeadLetter(eventId, "Reason"))
            .isInstanceOf(InvalidStateTransitionException.class)
            .hasMessageContaining("not eligible for replay");

        verify(outboxEventRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Replay throws 409 when concurrent replay claims race and update count is 0")
    void shouldThrow409WhenReplayRaceLost() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "payment.failed", "{}", WebhookStatus.DEAD_LETTER);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));
        when(webhookEventRepository.claimEventForReplay(eq(eventId), eq(WebhookStatus.DEAD_LETTER), eq(WebhookStatus.RECEIVED), any(Instant.class)))
            .thenReturn(0); // Lost race

        assertThatThrownBy(() -> deadLetterService.replayDeadLetter(eventId, "Reason"))
            .isInstanceOf(InvalidStateTransitionException.class)
            .hasMessageContaining("could not be claimed");

        verify(outboxEventRepository, never()).saveAndFlush(any());
        verify(webhookReplayRepository, never()).saveAndFlush(any());
    }
}
