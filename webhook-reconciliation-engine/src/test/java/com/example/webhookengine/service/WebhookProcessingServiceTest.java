package com.example.webhookengine.service;

import com.example.webhookengine.config.RetryProperties;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WebhookProcessingServiceTest {

    @Mock
    private WebhookEventRepository webhookEventRepository;

    @Mock
    private WebhookAttemptRepository webhookAttemptRepository;

    @Mock
    private WebhookEventProcessor webhookEventProcessor;

    private RetryProperties retryProperties;
    private RetryPolicy retryPolicy;
    private WebhookProcessingService service;

    @BeforeEach
    void setUp() {
        retryProperties = new RetryProperties();
        retryProperties.setEnabled(true);
        retryProperties.setMaxAttempts(3);
        retryProperties.setInitialDelayMs(1000L);
        retryProperties.setMultiplier(2.0);
        retryProperties.setMaxDelayMs(60000L);

        retryPolicy = new RetryPolicy(retryProperties);
        service = new WebhookProcessingService(webhookEventRepository, webhookAttemptRepository, webhookEventProcessor, retryPolicy);
    }

    @Test
    @DisplayName("Processes RECEIVED event, sets COMPLETED status, and records attempt")
    void shouldProcessReceivedEventSuccessfully() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.success", "{}", WebhookStatus.RECEIVED);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));

        service.processEventById(eventId);

        verify(webhookEventProcessor).processEvent(event);
        assertThat(event.getStatus()).isEqualTo(WebhookStatus.COMPLETED);
        assertThat(event.getAttemptCount()).isEqualTo(1);
        assertThat(event.getProcessedAt()).isNotNull();

        ArgumentCaptor<WebhookAttempt> attemptCaptor = ArgumentCaptor.forClass(WebhookAttempt.class);
        verify(webhookAttemptRepository).saveAndFlush(attemptCaptor.capture());
        assertThat(attemptCaptor.getValue().getStatus()).isEqualTo(WebhookStatus.COMPLETED);
        assertThat(attemptCaptor.getValue().getAttemptNumber()).isEqualTo(1);
    }

    @Test
    @DisplayName("Ignores COMPLETED event on duplicate message delivery")
    void shouldIgnoreAlreadyCompletedEvent() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.success", "{}", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));

        service.processEventById(eventId);

        verify(webhookEventProcessor, never()).processEvent(any());
        verify(webhookAttemptRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Ignores event already in PROCESSING status to prevent concurrent duplicate execution")
    void shouldIgnoreEventAlreadyInProcessing() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.success", "{}", WebhookStatus.PROCESSING);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));

        service.processEventById(eventId);

        verify(webhookEventProcessor, never()).processEvent(any());
        verify(webhookAttemptRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Safely handles message referencing missing database record")
    void shouldHandleMissingDatabaseEventGracefully() {
        UUID missingId = UUID.randomUUID();
        when(webhookEventRepository.findById(missingId)).thenReturn(Optional.empty());

        service.processEventById(missingId);

        verify(webhookEventProcessor, never()).processEvent(any());
        verify(webhookAttemptRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("Sets RETRY_PENDING status and calculates nextRetryAt when processing fails and attempts < maxAttempts")
    void shouldHandleRetryableProcessingFailure() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.success", "{}", WebhookStatus.RECEIVED);

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));
        doThrow(new RuntimeException("Transient connection reset")).when(webhookEventProcessor).processEvent(event);

        assertThatThrownBy(() -> service.processEventById(eventId))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Transient connection reset");

        // Attempt count incremented to 1 (failed execution 1 < maxAttempts 3)
        assertThat(event.getAttemptCount()).isEqualTo(1);
        assertThat(event.getStatus()).isEqualTo(WebhookStatus.RETRY_PENDING);
        assertThat(event.getNextRetryAt()).isNotNull();
        assertThat(event.getLastError()).isEqualTo("Transient connection reset");

        ArgumentCaptor<WebhookAttempt> attemptCaptor = ArgumentCaptor.forClass(WebhookAttempt.class);
        verify(webhookAttemptRepository).saveAndFlush(attemptCaptor.capture());
        assertThat(attemptCaptor.getValue().getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(attemptCaptor.getValue().getAttemptNumber()).isEqualTo(1);
        assertThat(attemptCaptor.getValue().getErrorMessage()).isEqualTo("Transient connection reset");
    }

    @Test
    @DisplayName("Sets FAILED status and clears nextRetryAt when processing fails on final attempt (attempts >= maxAttempts)")
    void shouldHandleExhaustedProcessingFailure() {
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = new WebhookEvent(eventId, "evt_1", "stripe", "charge.success", "{}", WebhookStatus.RECEIVED);
        event.setAttemptCount(2); // Already executed 2 times; this will be attempt 3 of 3 (maxAttempts)

        when(webhookEventRepository.findById(eventId)).thenReturn(Optional.of(event));
        doThrow(new RuntimeException("Permanent bad data")).when(webhookEventProcessor).processEvent(event);

        assertThatThrownBy(() -> service.processEventById(eventId))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Permanent bad data");

        // Attempt count incremented to 3 (failed execution 3 >= maxAttempts 3) -> DEAD_LETTER
        assertThat(event.getAttemptCount()).isEqualTo(3);
        assertThat(event.getStatus()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(event.getNextRetryAt()).isNull();
        assertThat(event.getLastError()).isEqualTo("Permanent bad data");

        ArgumentCaptor<WebhookAttempt> attemptCaptor = ArgumentCaptor.forClass(WebhookAttempt.class);
        verify(webhookAttemptRepository).saveAndFlush(attemptCaptor.capture());
        assertThat(attemptCaptor.getValue().getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(attemptCaptor.getValue().getAttemptNumber()).isEqualTo(3);
        assertThat(attemptCaptor.getValue().getErrorMessage()).isEqualTo("Permanent bad data");
    }
}
