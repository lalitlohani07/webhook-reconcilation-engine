package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.WebhookStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

@Schema(description = "Request parameters for initiating a bounded reconciliation run")
public record ReconciliationRunRequest(
    @Min(value = 1, message = "batchSize must be at least 1")
    @Max(value = 100, message = "batchSize cannot exceed 100")
    @Schema(description = "Maximum number of events to reconcile in this batch (1-100)", example = "50", defaultValue = "50")
    Integer batchSize,

    @Schema(description = "Optional filter by webhook source (e.g., stripe, shopify)", example = "stripe")
    String source,

    @Schema(description = "Optional filter by webhook event status (must be terminal status: COMPLETED, DEAD_LETTER, FAILED)", example = "COMPLETED")
    WebhookStatus status,

    @Schema(description = "Whether to re-reconcile events that already have a reconciliation record", example = "false", defaultValue = "false")
    Boolean includeAlreadyReconciled
) {
    public ReconciliationRunRequest(Integer batchSize) {
        this(batchSize, null, null, false);
    }

    public ReconciliationRunRequest() {
        this(null, null, null, false);
    }

    public int resolvedBatchSize(int defaultSize, int maxSize) {
        if (batchSize == null || batchSize <= 0) {
            return defaultSize;
        }
        return Math.min(batchSize, maxSize);
    }
}
