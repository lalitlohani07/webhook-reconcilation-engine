package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.WebhookStatus;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Response returned upon webhook ingestion")
public class WebhookIngestResponse {

    @Schema(description = "External event identifier", example = "evt_payment_123")
    private String eventId;

    @Schema(description = "Source provider or system", example = "payment-service")
    private String source;

    @Schema(description = "Current lifecycle status of the webhook event", example = "RECEIVED")
    private WebhookStatus status;

    @Schema(description = "Informative status message", example = "Webhook accepted for processing")
    private String message;

    public WebhookIngestResponse() {
    }

    public WebhookIngestResponse(String eventId, String source, WebhookStatus status, String message) {
        this.eventId = eventId;
        this.source = source;
        this.status = status;
        this.message = message;
    }

    public static WebhookIngestResponse accepted(String eventId, String source, WebhookStatus status) {
        return new WebhookIngestResponse(eventId, source, status, "Webhook accepted for processing");
    }

    public static WebhookIngestResponse duplicate(String eventId, String source, WebhookStatus status) {
        return new WebhookIngestResponse(eventId, source, status, "Webhook event already received");
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public WebhookStatus getStatus() {
        return status;
    }

    public void setStatus(WebhookStatus status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
