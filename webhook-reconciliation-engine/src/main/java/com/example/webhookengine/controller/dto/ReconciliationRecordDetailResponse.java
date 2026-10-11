package com.example.webhookengine.controller.dto;

import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Detailed representation of an individual reconciliation record")
public record ReconciliationRecordDetailResponse(
    @Schema(description = "Internal unique ID of the reconciliation record")
    UUID id,

    @Schema(description = "Internal UUID of the reconciled webhook event")
    UUID webhookEventId,

    @Schema(description = "External event ID from the webhook provider")
    String eventId,

    @Schema(description = "Source provider name")
    String source,

    @Schema(description = "Event type / action")
    String eventType,

    @Schema(description = "Reported status from the external source of truth")
    String externalStatus,

    @Schema(description = "Internal system status of the webhook event at reconciliation time")
    String internalStatus,

    @Schema(description = "Reconciliation outcome classification (MATCHED, CORRECTED, MISMATCH, FAILED)")
    ReconciliationResult result,

    @Schema(description = "Timestamp when the reconciliation was performed")
    Instant reconciledAt,

    @Schema(description = "Structured JSON details explaining comparison findings, discrepancies, or failure reasons")
    JsonNode details
) {
    public static ReconciliationRecordDetailResponse fromEntity(ReconciliationRecord record, ObjectMapper objectMapper) {
        JsonNode parsedDetails = null;
        if (record.getDetails() != null && !record.getDetails().isBlank()) {
            try {
                parsedDetails = objectMapper.readTree(record.getDetails());
            } catch (Exception ignored) {
            }
        }
        return new ReconciliationRecordDetailResponse(
            record.getId(),
            record.getWebhookEvent().getId(),
            record.getWebhookEvent().getEventId(),
            record.getWebhookEvent().getSource(),
            record.getWebhookEvent().getEventType(),
            record.getExternalStatus(),
            record.getInternalStatus(),
            record.getResult(),
            record.getReconciledAt(),
            parsedDetails
        );
    }
}
