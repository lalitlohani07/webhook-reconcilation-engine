package com.example.webhookengine.messaging;

import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private WebhookEventPublisher webhookEventPublisher;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    private OutboxPublisher outboxPublisher;

    @BeforeEach
    void setUp() {
        outboxPublisher = new OutboxPublisher(outboxEventRepository, webhookEventPublisher, objectMapper);
    }

    @Test
    @DisplayName("Discovers pending outbox events, publishes to broker, and marks record PUBLISHED upon confirmation")
    void shouldPublishPendingEventAndMarkPublished() throws Exception {
        UUID outboxId = UUID.randomUUID();
        UUID webhookEventId = UUID.randomUUID();

        WebhookProcessingMessage message = new WebhookProcessingMessage(
            webhookEventId,
            "evt_pub_1",
            "stripe",
            "charge.succeeded"
        );
        String payloadJson = objectMapper.writeValueAsString(message);

        OutboxEvent outboxEvent = new OutboxEvent(outboxId, webhookEventId.toString(), "charge.succeeded", payloadJson, OutboxStatus.PENDING);

        when(outboxEventRepository.findPendingEventsForProcessing(eq(OutboxStatus.PENDING), any(Pageable.class)))
            .thenReturn(List.of(outboxEvent));
        when(outboxEventRepository.findById(outboxId))
            .thenReturn(Optional.of(outboxEvent));

        outboxPublisher.processOutboxEvents();

        verify(webhookEventPublisher, times(1)).publishProcessingMessage(eq(message));
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(outboxEvent.getPublishedAt()).isNotNull();
        verify(outboxEventRepository, times(1)).saveAndFlush(outboxEvent);
    }

    @Test
    @DisplayName("Publication failure or broker rejection leaves outbox record PENDING for retry and increments attemptCount")
    void shouldLeaveEventPendingWhenPublicationFails() throws Exception {
        UUID outboxId = UUID.randomUUID();
        UUID webhookEventId = UUID.randomUUID();

        WebhookProcessingMessage message = new WebhookProcessingMessage(
            webhookEventId,
            "evt_pub_2",
            "stripe",
            "charge.failed"
        );
        String payloadJson = objectMapper.writeValueAsString(message);

        OutboxEvent outboxEvent = new OutboxEvent(outboxId, webhookEventId.toString(), "charge.failed", payloadJson, OutboxStatus.PENDING);

        when(outboxEventRepository.findPendingEventsForProcessing(eq(OutboxStatus.PENDING), any(Pageable.class)))
            .thenReturn(List.of(outboxEvent));
        when(outboxEventRepository.findById(outboxId))
            .thenReturn(Optional.of(outboxEvent));

        doThrow(new WebhookPublishingException("Broker rejected message with NACK"))
            .when(webhookEventPublisher).publishProcessingMessage(any());

        outboxPublisher.processOutboxEvents();

        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getAttemptCount()).isEqualTo(1);
        assertThat(outboxEvent.getLastError()).contains("Broker rejected message with NACK");
        assertThat(outboxEvent.getPublishedAt()).isNull();
        verify(outboxEventRepository, times(1)).saveAndFlush(outboxEvent);
    }

    @Test
    @DisplayName("Unroutable message failure leaves outbox record PENDING and not PUBLISHED")
    void shouldNotMarkPublishedWhenMessageIsUnroutable() throws Exception {
        UUID outboxId = UUID.randomUUID();
        UUID webhookEventId = UUID.randomUUID();

        WebhookProcessingMessage message = new WebhookProcessingMessage(
            webhookEventId,
            "evt_pub_3",
            "stripe",
            "charge.refunded"
        );
        String payloadJson = objectMapper.writeValueAsString(message);

        OutboxEvent outboxEvent = new OutboxEvent(outboxId, webhookEventId.toString(), "charge.refunded", payloadJson, OutboxStatus.PENDING);

        when(outboxEventRepository.findPendingEventsForProcessing(eq(OutboxStatus.PENDING), any(Pageable.class)))
            .thenReturn(List.of(outboxEvent));
        when(outboxEventRepository.findById(outboxId))
            .thenReturn(Optional.of(outboxEvent));

        doThrow(new WebhookPublishingException("Message is unroutable: NO_ROUTE"))
            .when(webhookEventPublisher).publishProcessingMessage(any());

        outboxPublisher.processOutboxEvents();

        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getLastError()).contains("NO_ROUTE");
        assertThat(outboxEvent.getPublishedAt()).isNull();
    }
}
