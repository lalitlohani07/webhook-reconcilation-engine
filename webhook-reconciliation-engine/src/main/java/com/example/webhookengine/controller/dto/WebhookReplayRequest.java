package com.example.webhookengine.controller.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Size;

@Schema(description = "Request body for replaying a dead-letter event")
public record WebhookReplayRequest(
    @Schema(description = "Optional reason or operational justification for replaying the event", example = "Transient dependency issue has been resolved")
    @Size(max = 500, message = "Reason cannot exceed 500 characters")
    String reason
) {
}
