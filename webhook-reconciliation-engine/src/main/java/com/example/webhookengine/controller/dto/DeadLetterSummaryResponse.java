package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.WebhookStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Summary representation of a dead-letter webhook event")
public record DeadLetterSummaryResponse(
    @Schema(description = "Internal unique UUID of the event")
    UUID id,

    @Schema(description = "External event ID from the source provider")
    String eventId,

    @Schema(description = "Source platform or provider")
    String source,

    @Schema(description = "Type of webhook event")
    String eventType,

    @Schema(description = "Current lifecycle status", example = "DEAD_LETTER")
    WebhookStatus status,

    @Schema(description = "Total number of processing attempts executed")
    int attemptCount,

    @Schema(description = "Final error message leading to dead-letter state")
    String lastError,

    @Schema(description = "Timestamp when the event was initially received")
    Instant createdAt,

    @Schema(description = "Timestamp when the event was last updated")
    Instant updatedAt
) {
}
