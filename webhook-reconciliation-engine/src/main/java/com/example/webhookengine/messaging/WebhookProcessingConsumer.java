package com.example.webhookengine.messaging;

import com.example.webhookengine.service.WebhookProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class WebhookProcessingConsumer {

    private static final Logger log = LoggerFactory.getLogger(WebhookProcessingConsumer.class);

    private final WebhookProcessingService webhookProcessingService;

    public WebhookProcessingConsumer(WebhookProcessingService webhookProcessingService) {
        this.webhookProcessingService = webhookProcessingService;
    }

    @RabbitListener(queues = "${app.rabbitmq.queue:webhook.processing}")
    public void handleWebhookProcessingMessage(WebhookProcessingMessage message) {
        if (message == null || message.webhookEventId() == null) {
            log.warn("Received invalid WebhookProcessingMessage. Discarding message.");
            return;
        }

        log.info("Received WebhookProcessingMessage from queue: internalId={}, eventId={}, source={}, eventType={}",
            message.webhookEventId(), message.eventId(), message.source(), message.eventType());

        try {
            webhookProcessingService.processEventById(message.webhookEventId());
        } catch (Exception ex) {
            log.error("Exception thrown while handling message for internalId={}: {}. Message will not be requeued (bounded error policy).",
                message.webhookEventId(), ex.getMessage());
            // In accordance with Phase 3 specifications:
            // default-requeue-rejected is false to prevent infinite immediate requeue loops.
            // Retries & DLQ will be introduced in subsequent phases.
        }
    }
}
