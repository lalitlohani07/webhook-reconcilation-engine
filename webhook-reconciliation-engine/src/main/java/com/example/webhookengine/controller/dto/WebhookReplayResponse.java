package com.example.webhookengine.controller.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Details of an event replay operation")
public record WebhookReplayResponse(
    @Schema(description = "Internal unique UUID of the replay record")
    UUID id,

    @Schema(description = "Timestamp when the replay was requested")
    Instant requestedAt,

    @Schema(description = "Operator-provided reason for replay")
    String reason,

    @Schema(description = "Attempt count at the time replay was initiated")
    int previousAttemptCount
) {
}
