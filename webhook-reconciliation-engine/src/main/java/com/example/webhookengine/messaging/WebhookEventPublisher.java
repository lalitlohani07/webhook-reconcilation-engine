package com.example.webhookengine.messaging;

import com.example.webhookengine.config.RabbitMqConfig;
import com.example.webhookengine.domain.WebhookEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class WebhookEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WebhookEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMqConfig rabbitMqConfig;

    public WebhookEventPublisher(RabbitTemplate rabbitTemplate, RabbitMqConfig rabbitMqConfig) {
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitMqConfig = rabbitMqConfig;
    }

    /**
     * Publishes a processing job message for a newly accepted webhook event.
     *
     * @param event the persisted WebhookEvent
     */
    public void publishWebhookEvent(WebhookEvent event) {
        WebhookProcessingMessage message = new WebhookProcessingMessage(
            event.getId(),
            event.getEventId(),
            event.getSource(),
            event.getEventType()
        );
        publishProcessingMessage(message);
    }

    /**
     * Publishes WebhookProcessingMessage and waits for broker confirmation (ACK/NACK and returns).
     *
     * @param message the WebhookProcessingMessage DTO
     */
    public void publishProcessingMessage(WebhookProcessingMessage message) {
        String exchange = rabbitMqConfig.getExchangeName();
        String routingKey = rabbitMqConfig.getRoutingKey();
        CorrelationData correlationData = new CorrelationData(message.webhookEventId().toString());

        try {
            log.info("Publishing webhook processing message to exchange='{}', routingKey='{}' for internalId={}, eventId={}, source={}",
                exchange, routingKey, message.webhookEventId(), message.eventId(), message.source());

            rabbitTemplate.convertAndSend(exchange, routingKey, message, correlationData);

            // Wait for publisher confirm from broker if confirms are enabled
            CorrelationData.Confirm confirm = correlationData.getFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (!confirm.isAck()) {
                String cause = confirm.getReason();
                log.error("Broker returned NACK for internalId={}: reason={}", message.webhookEventId(), cause);
                throw new WebhookPublishingException("Broker rejected message with NACK: " + cause);
            }

            // Check if message was returned as unroutable
            if (correlationData.getReturned() != null) {
                log.error("Broker returned unroutable message for internalId={}: replyCode={}, replyText={}",
                    message.webhookEventId(),
                    correlationData.getReturned().getReplyCode(),
                    correlationData.getReturned().getReplyText());
                throw new WebhookPublishingException("Message is unroutable: " + correlationData.getReturned().getReplyText());
            }

            log.debug("Published and confirmed message successfully for internalId={}", message.webhookEventId());
        } catch (WebhookPublishingException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to publish webhook processing message to RabbitMQ for internalId={}, eventId={}, source={}: {}",
                message.webhookEventId(), message.eventId(), message.source(), e.getMessage(), e);
            throw new WebhookPublishingException("Failed to publish webhook event to message broker", e);
        }
    }
}
