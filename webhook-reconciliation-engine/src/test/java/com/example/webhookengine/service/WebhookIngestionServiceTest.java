package com.example.webhookengine.service;

import com.example.webhookengine.controller.dto.WebhookIngestRequest;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WebhookIngestionServiceTest {

    @Mock
    private WebhookEventRepository webhookEventRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    private WebhookIngestionService service;

    @BeforeEach
    void setUp() {
        service = new WebhookIngestionService(webhookEventRepository, outboxEventRepository, objectMapper);
    }

    @Test
    @DisplayName("New webhook event saves WebhookEvent and OutboxEvent atomically with isNew = true")
    void shouldSaveNewWebhookEventAndOutboxEvent() {
        WebhookIngestRequest request = new WebhookIngestRequest(
            "evt_100",
            "stripe",
            "payment.succeeded",
            objectMapper.createObjectNode().put("amount", 200)
        );

        when(webhookEventRepository.findBySourceAndEventId("stripe", "evt_100"))
            .thenReturn(Optional.empty());

        WebhookEvent savedEvent = new WebhookEvent("evt_100", "stripe", "payment.succeeded", "{\"amount\":200}", WebhookStatus.RECEIVED);
        when(webhookEventRepository.saveAndFlush(any(WebhookEvent.class))).thenReturn(savedEvent);

        IngestionResult result = service.ingestWebhook(request);

        assertThat(result.isNew()).isTrue();
        assertThat(result.event().getEventId()).isEqualTo("evt_100");
        assertThat(result.event().getSource()).isEqualTo("stripe");
        assertThat(result.event().getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        verify(webhookEventRepository, times(1)).saveAndFlush(any(WebhookEvent.class));

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository, times(1)).saveAndFlush(outboxCaptor.capture());
        OutboxEvent capturedOutbox = outboxCaptor.getValue();
        assertThat(capturedOutbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(capturedOutbox.getEventType()).isEqualTo("payment.succeeded");
        assertThat(capturedOutbox.getPayload()).contains("evt_100");
    }

    @Test
    @DisplayName("Sequential duplicate returns existing event without creating OutboxEvent")
    void shouldReturnExistingEventOnSequentialDuplicate() {
        WebhookIngestRequest request = new WebhookIngestRequest(
            "evt_100",
            "stripe",
            "payment.succeeded",
            objectMapper.createObjectNode().put("amount", 200)
        );

        WebhookEvent existingEvent = new WebhookEvent("evt_100", "stripe", "payment.succeeded", "{\"amount\":200}", WebhookStatus.PROCESSING);
        when(webhookEventRepository.findBySourceAndEventId("stripe", "evt_100"))
            .thenReturn(Optional.of(existingEvent));

        IngestionResult result = service.ingestWebhook(request);

        assertThat(result.isNew()).isFalse();
        assertThat(result.event().getStatus()).isEqualTo(WebhookStatus.PROCESSING);
        verify(webhookEventRepository, never()).saveAndFlush(any(WebhookEvent.class));
        verify(outboxEventRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Concurrent duplicate catching DataIntegrityViolationException returns existing record without creating OutboxEvent")
    void shouldHandleConcurrentDuplicateGracefully() {
        WebhookIngestRequest request = new WebhookIngestRequest(
            "evt_100",
            "stripe",
            "payment.succeeded",
            objectMapper.createObjectNode().put("amount", 200)
        );

        WebhookEvent existingEvent = new WebhookEvent("evt_100", "stripe", "payment.succeeded", "{\"amount\":200}", WebhookStatus.RECEIVED);

        when(webhookEventRepository.findBySourceAndEventId("stripe", "evt_100"))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(existingEvent));

        when(webhookEventRepository.saveAndFlush(any(WebhookEvent.class)))
            .thenThrow(new DataIntegrityViolationException("Duplicate key constraint violated"));

        IngestionResult result = service.ingestWebhook(request);

        assertThat(result.isNew()).isFalse();
        assertThat(result.event().getEventId()).isEqualTo("evt_100");
        verify(webhookEventRepository, times(2)).findBySourceAndEventId("stripe", "evt_100");
        verify(outboxEventRepository, never()).saveAndFlush(any());
    }
}
