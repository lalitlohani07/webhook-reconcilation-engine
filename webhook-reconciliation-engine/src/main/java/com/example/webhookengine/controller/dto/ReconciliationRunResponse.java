package com.example.webhookengine.controller.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Schema(description = "Summary report returned upon completing a reconciliation run batch")
public record ReconciliationRunResponse(
    @Schema(description = "Unique batch identifier for this reconciliation run")
    UUID runId,

    @Schema(description = "Execution start timestamp")
    Instant startedAt,

    @Schema(description = "Execution completion timestamp")
    Instant completedAt,

    @Schema(description = "Total number of webhook events evaluated in this run")
    int totalEvaluated,

    @Schema(description = "Number of events classified as MATCHED")
    int matchedCount,

    @Schema(description = "Number of events classified as MISMATCH")
    int mismatchCount,

    @Schema(description = "Number of events classified as CORRECTED")
    int correctedCount,

    @Schema(description = "Number of events classified as FAILED")
    int failedCount,

    @Schema(description = "List of reconciliation record summaries generated in this run")
    List<ReconciliationRecordSummaryResponse> records
) {
    public ReconciliationRunResponse(
        UUID runId,
        Instant startedAt,
        Instant completedAt,
        int totalEvaluated,
        int matchedCount,
        int mismatchCount,
        int correctedCount,
        int failedCount
    ) {
        this(runId, startedAt, completedAt, totalEvaluated, matchedCount, mismatchCount, correctedCount, failedCount, java.util.Collections.emptyList());
    }
}
