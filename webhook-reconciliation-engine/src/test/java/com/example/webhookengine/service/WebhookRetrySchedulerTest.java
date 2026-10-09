package com.example.webhookengine.service;

import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.messaging.WebhookProcessingMessage;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WebhookRetrySchedulerTest {

    @Mock
    private WebhookEventRepository webhookEventRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private ObjectMapper objectMapper;
    private WebhookRetryScheduler scheduler;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        scheduler = new WebhookRetryScheduler(webhookEventRepository, outboxEventRepository, objectMapper);
        scheduler.setBatchSize(10);
    }

    @Test
    @DisplayName("Claims due eligible event and creates corresponding PENDING OutboxEvent")
    void shouldClaimAndEnqueueDueRetry() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_due_1", "stripe", "payment.failed", "{\"amount\":100}", WebhookStatus.RETRY_PENDING);
        event.setAttemptCount(1);
        event.setNextRetryAt(Instant.now().minusSeconds(10));

        when(webhookEventRepository.findDueEventsForRetry(eq(WebhookStatus.RETRY_PENDING), any(Instant.class), any(Pageable.class)))
            .thenReturn(List.of(event));

        when(webhookEventRepository.claimEventForRetry(eq(eventId), eq(WebhookStatus.RETRY_PENDING), eq(WebhookStatus.RECEIVED), any(Instant.class)))
            .thenReturn(1);

        scheduler.scheduleDueRetries();

        verify(webhookEventRepository).claimEventForRetry(eq(eventId), eq(WebhookStatus.RETRY_PENDING), eq(WebhookStatus.RECEIVED), any(Instant.class));

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).saveAndFlush(outboxCaptor.capture());

        OutboxEvent outbox = outboxCaptor.getValue();
        assertThat(outbox.getAggregateId()).isEqualTo(eventId.toString());
        assertThat(outbox.getEventType()).isEqualTo("payment.failed");
        assertThat(outbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outbox.getPayload()).contains(eventId.toString());
        assertThat(outbox.getPayload()).contains("evt_due_1");
    }

    @Test
    @DisplayName("Does not create OutboxEvent if another scheduler instance already claimed the event (updated = 0)")
    void shouldNotEnqueueIfClaimRaceLost() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_race", "stripe", "payment.failed", "{}", WebhookStatus.RETRY_PENDING);

        when(webhookEventRepository.findDueEventsForRetry(eq(WebhookStatus.RETRY_PENDING), any(Instant.class), any(Pageable.class)))
            .thenReturn(List.of(event));

        // Simulating another node already updated the status
        when(webhookEventRepository.claimEventForRetry(eq(eventId), eq(WebhookStatus.RETRY_PENDING), eq(WebhookStatus.RECEIVED), any(Instant.class)))
            .thenReturn(0);

        scheduler.scheduleDueRetries();

        verify(outboxEventRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Does nothing when no eligible events are due")
    void shouldDoNothingWhenNoEventsDue() {
        when(webhookEventRepository.findDueEventsForRetry(eq(WebhookStatus.RETRY_PENDING), any(Instant.class), any(Pageable.class)))
            .thenReturn(Collections.emptyList());

        scheduler.scheduleDueRetries();

        verify(webhookEventRepository, never()).claimEventForRetry(any(), any(), any(), any());
        verify(outboxEventRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Handles multiple eligible events in a batch up to configured batch size")
    void shouldHandleBatchOfDueEvents() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        WebhookEvent e1 = new WebhookEvent(id1, "evt_1", "stripe", "charge.failed", "{}", WebhookStatus.RETRY_PENDING);
        WebhookEvent e2 = new WebhookEvent(id2, "evt_2", "stripe", "charge.failed", "{}", WebhookStatus.RETRY_PENDING);

        when(webhookEventRepository.findDueEventsForRetry(eq(WebhookStatus.RETRY_PENDING), any(Instant.class), any(Pageable.class)))
            .thenReturn(List.of(e1, e2));

        when(webhookEventRepository.claimEventForRetry(any(), eq(WebhookStatus.RETRY_PENDING), eq(WebhookStatus.RECEIVED), any(Instant.class)))
            .thenReturn(1);

        scheduler.scheduleDueRetries();

        verify(outboxEventRepository, org.mockito.Mockito.times(2)).saveAndFlush(any());
    }
}
