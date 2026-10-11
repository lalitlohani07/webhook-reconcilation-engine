package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.WebhookStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.UUID;

@Schema(description = "Response returned after successfully accepting an event replay request")
public record WebhookReplayResponsePayload(
    @Schema(description = "Internal unique UUID of the event")
    UUID id,

    @Schema(description = "External event ID from the source provider")
    String eventId,

    @Schema(description = "Source platform or provider")
    String source,

    @Schema(description = "Current status of the event after re-enqueueing", example = "RECEIVED")
    WebhookStatus status,

    @Schema(description = "Confirmation message indicating replay was accepted", example = "Webhook event replay accepted and enqueued for processing")
    String message
) {
}
