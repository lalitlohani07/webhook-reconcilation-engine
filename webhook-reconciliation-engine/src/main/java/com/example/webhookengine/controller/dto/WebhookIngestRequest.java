package com.example.webhookengine.controller.dto;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Schema(description = "Request payload for ingesting a webhook event")
public class WebhookIngestRequest {

    @NotBlank(message = "eventId is required and cannot be blank")
    @Size(max = 255, message = "eventId cannot exceed 255 characters")
    @Schema(description = "External event identifier from the webhook provider", example = "evt_payment_123", requiredMode = Schema.RequiredMode.REQUIRED)
    private String eventId;

    @NotBlank(message = "source is required and cannot be blank")
    @Size(max = 100, message = "source cannot exceed 100 characters")
    @Schema(description = "Source provider or system sending the webhook", example = "payment-service", requiredMode = Schema.RequiredMode.REQUIRED)
    private String source;

    @NotBlank(message = "eventType is required and cannot be blank")
    @Size(max = 100, message = "eventType cannot exceed 100 characters")
    @Schema(description = "Type of the webhook event", example = "PAYMENT_COMPLETED", requiredMode = Schema.RequiredMode.REQUIRED)
    private String eventType;

    @NotNull(message = "payload is required and must be a valid JSON object")
    @Schema(description = "Arbitrary JSON payload of the webhook event", example = "{\"paymentId\": \"pay_456\", \"amount\": 1499.00, \"currency\": \"INR\", \"status\": \"COMPLETED\"}", requiredMode = Schema.RequiredMode.REQUIRED)
    private JsonNode payload;

    public WebhookIngestRequest() {
    }

    public WebhookIngestRequest(String eventId, String source, String eventType, JsonNode payload) {
        this.eventId = eventId;
        this.source = source;
        this.eventType = eventType;
        this.payload = payload;
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

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
}
