package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.WebhookStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Details of an individual processing attempt")
public record WebhookAttemptResponse(
    @Schema(description = "Internal unique UUID of the attempt")
    UUID id,

    @Schema(description = "Attempt sequence number (1-based)")
    int attemptNumber,

    @Schema(description = "Status of the attempt", example = "FAILED")
    WebhookStatus status,

    @Schema(description = "Timestamp when execution started")
    Instant startedAt,

    @Schema(description = "Timestamp when execution completed or failed")
    Instant completedAt,

    @Schema(description = "Error message encountered during the attempt")
    String errorMessage
) {
}
