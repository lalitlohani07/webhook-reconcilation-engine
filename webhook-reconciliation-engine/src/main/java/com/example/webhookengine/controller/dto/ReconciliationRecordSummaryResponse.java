package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Summary of a reconciliation outcome for a webhook event")
public record ReconciliationRecordSummaryResponse(
    @Schema(description = "Internal unique ID of the reconciliation record")
    UUID id,

    @Schema(description = "Internal UUID of the reconciled webhook event")
    UUID webhookEventId,

    @Schema(description = "External event ID from the webhook provider")
    String eventId,

    @Schema(description = "Source provider name")
    String source,

    @Schema(description = "Reported status from the external source of truth")
    String externalStatus,

    @Schema(description = "Internal system status of the webhook event at reconciliation time")
    String internalStatus,

    @Schema(description = "Reconciliation outcome classification (MATCHED, CORRECTED, MISMATCH, FAILED)")
    ReconciliationResult result,

    @Schema(description = "Timestamp when the reconciliation was performed")
    Instant reconciledAt
) {
    public static ReconciliationRecordSummaryResponse fromEntity(ReconciliationRecord record) {
        return new ReconciliationRecordSummaryResponse(
            record.getId(),
            record.getWebhookEvent().getId(),
            record.getWebhookEvent().getEventId(),
            record.getWebhookEvent().getSource(),
            record.getExternalStatus(),
            record.getInternalStatus(),
            record.getResult(),
            record.getReconciledAt()
        );
    }
}
