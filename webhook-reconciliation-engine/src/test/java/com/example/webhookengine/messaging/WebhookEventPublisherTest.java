package com.example.webhookengine.messaging;

import com.example.webhookengine.config.RabbitMqConfig;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WebhookEventPublisherTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private RabbitMqConfig rabbitMqConfig;

    private WebhookEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new WebhookEventPublisher(rabbitTemplate, rabbitMqConfig);
    }

    @Test
    @DisplayName("Publishes message with internalId and metadata to configured exchange and routing key")
    void shouldPublishMessageWithCorrectMetadata() {
        when(rabbitMqConfig.getExchangeName()).thenReturn("webhook.events");
        when(rabbitMqConfig.getRoutingKey()).thenReturn("webhook.received");

        WebhookEvent event = new WebhookEvent("evt_abc", "stripe", "charge.succeeded", "{\"amount\":100}", WebhookStatus.RECEIVED);

        org.mockito.Mockito.doAnswer(invocation -> {
            CorrelationData cd = invocation.getArgument(3);
            if (cd != null) {
                cd.getFuture().complete(new CorrelationData.Confirm(true, null));
            }
            return null;
        }).when(rabbitTemplate).convertAndSend(
            eq("webhook.events"),
            eq("webhook.received"),
            any(WebhookProcessingMessage.class),
            any(CorrelationData.class)
        );

        publisher.publishWebhookEvent(event);

        ArgumentCaptor<WebhookProcessingMessage> messageCaptor = ArgumentCaptor.forClass(WebhookProcessingMessage.class);
        ArgumentCaptor<CorrelationData> correlationCaptor = ArgumentCaptor.forClass(CorrelationData.class);

        verify(rabbitTemplate).convertAndSend(
            eq("webhook.events"),
            eq("webhook.received"),
            messageCaptor.capture(),
            correlationCaptor.capture()
        );

        WebhookProcessingMessage message = messageCaptor.getValue();
        assertThat(message.webhookEventId()).isEqualTo(event.getId());
        assertThat(message.eventId()).isEqualTo("evt_abc");
        assertThat(message.source()).isEqualTo("stripe");
        assertThat(message.eventType()).isEqualTo("charge.succeeded");

        assertThat(correlationCaptor.getValue().getId()).isEqualTo(event.getId().toString());
    }

    @Test
    @DisplayName("Throws WebhookPublishingException when rabbitTemplate throws an exception")
    void shouldThrowWebhookPublishingExceptionOnFailure() {
        when(rabbitMqConfig.getExchangeName()).thenReturn("webhook.events");
        when(rabbitMqConfig.getRoutingKey()).thenReturn("webhook.received");

        WebhookEvent event = new WebhookEvent("evt_fail", "stripe", "charge.failed", "{}", WebhookStatus.RECEIVED);

        doThrow(new RuntimeException("RabbitMQ connection down"))
            .when(rabbitTemplate).convertAndSend(any(String.class), any(String.class), any(Object.class), any(CorrelationData.class));

        assertThatThrownBy(() -> publisher.publishWebhookEvent(event))
            .isInstanceOf(WebhookPublishingException.class)
            .hasMessageContaining("Failed to publish webhook event");
    }
}
