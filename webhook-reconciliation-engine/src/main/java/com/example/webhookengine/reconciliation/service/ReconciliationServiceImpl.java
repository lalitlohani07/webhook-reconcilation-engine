package com.example.webhookengine.reconciliation.service;

import com.example.webhookengine.controller.dto.ReconciliationRecordDetailResponse;
import com.example.webhookengine.controller.dto.ReconciliationRecordSummaryResponse;
import com.example.webhookengine.controller.dto.ReconciliationRunRequest;
import com.example.webhookengine.controller.dto.ReconciliationRunResponse;
import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.ResourceNotFoundException;
import com.example.webhookengine.reconciliation.config.ReconciliationProperties;
import com.example.webhookengine.reconciliation.provider.ExternalEventRecord;
import com.example.webhookengine.reconciliation.provider.ExternalProviderException;
import com.example.webhookengine.reconciliation.provider.SourceOfTruthClient;
import com.example.webhookengine.repository.ReconciliationRecordRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Service
public class ReconciliationServiceImpl implements ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationServiceImpl.class);

    private static final Set<WebhookStatus> ELIGIBLE_STATUSES = Set.of(
        WebhookStatus.COMPLETED,
        WebhookStatus.DEAD_LETTER,
        WebhookStatus.FAILED
    );

    private static final Set<String> MATCHING_SUCCESS_STATUSES = Set.of(
        "COMPLETED", "PAID", "SUCCEEDED", "SUCCESS", "DELIVERED"
    );

    private static final Set<String> MATCHING_FAILURE_STATUSES = Set.of(
        "FAILED", "DEAD_LETTER", "CANCELLED", "REJECTED", "ERROR"
    );

    private static final Set<String> SAFE_CORRECTION_STATUSES = Set.of(
        "SETTLED_WITH_ADJUSTMENT", "RESOLVED_EXTERNALLY", "MANUALLY_SETTLED"
    );

    private final WebhookEventRepository webhookEventRepository;
    private final ReconciliationRecordRepository reconciliationRecordRepository;
    private final SourceOfTruthClient sourceOfTruthClient;
    private final ReconciliationProperties properties;
    private final ObjectMapper objectMapper;

    public ReconciliationServiceImpl(
        WebhookEventRepository webhookEventRepository,
        ReconciliationRecordRepository reconciliationRecordRepository,
        SourceOfTruthClient sourceOfTruthClient,
        ReconciliationProperties properties,
        ObjectMapper objectMapper
    ) {
        this.webhookEventRepository = webhookEventRepository;
        this.reconciliationRecordRepository = reconciliationRecordRepository;
        this.sourceOfTruthClient = sourceOfTruthClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public ReconciliationRunResponse runReconciliation(ReconciliationRunRequest request) {
        Instant startedAt = Instant.now();
        UUID runId = UUID.randomUUID();

        // 1. Resolve batch bounds
        int batchSize = request != null
            ? request.resolvedBatchSize(properties.getDefaultBatchSize(), properties.getMaxBatchSize())
            : properties.getDefaultBatchSize();

        if (batchSize > properties.getMaxBatchSize()) {
            throw new IllegalArgumentException("Batch size " + batchSize + " exceeds maximum allowed: " + properties.getMaxBatchSize());
        }

        // 2. Resolve status filtering
        List<WebhookStatus> statuses;
        if (request != null && request.status() != null) {
            if (!ELIGIBLE_STATUSES.contains(request.status())) {
                throw new IllegalArgumentException("Requested status " + request.status() + " is not eligible for reconciliation. Eligible statuses: " + ELIGIBLE_STATUSES);
            }
            statuses = List.of(request.status());
        } else {
            statuses = new ArrayList<>(ELIGIBLE_STATUSES);
        }

        String source = (request != null && request.source() != null && !request.source().isBlank())
            ? request.source().trim()
            : null;

        boolean includeAlreadyReconciled = request != null && Boolean.TRUE.equals(request.includeAlreadyReconciled());

        // 3. Select eligible events
        Pageable pageable = PageRequest.of(0, batchSize);
        List<WebhookEvent> eligibleEvents = includeAlreadyReconciled
            ? webhookEventRepository.findEligibleEvents(statuses, source, pageable)
            : webhookEventRepository.findEligibleUnreconciledEvents(statuses, source, pageable);

        log.info("Starting reconciliation run {}: found {} eligible events (batchSize={}, includeAlreadyReconciled={})",
            runId, eligibleEvents.size(), batchSize, includeAlreadyReconciled);

        int matchedCount = 0;
        int mismatchCount = 0;
        int correctedCount = 0;
        int failedCount = 0;
        List<ReconciliationRecordSummaryResponse> recordSummaries = new ArrayList<>();

        // 4. Reconcile each event individually without aborting the batch on failures
        for (WebhookEvent event : eligibleEvents) {
            try {
                ReconciliationRecord record = reconcileSingleEvent(event, includeAlreadyReconciled);
                recordSummaries.add(new ReconciliationRecordSummaryResponse(
                    record.getId(),
                    event.getId(),
                    event.getEventId(),
                    event.getSource(),
                    record.getExternalStatus(),
                    record.getInternalStatus(),
                    record.getResult(),
                    record.getReconciledAt()
                ));

                switch (record.getResult()) {
                    case MATCHED -> matchedCount++;
                    case MISMATCH -> mismatchCount++;
                    case CORRECTED -> correctedCount++;
                    case FAILED -> failedCount++;
                }
            } catch (Exception ex) {
                log.error("Unexpected error reconciling event internalId={}, eventId={}: {}",
                    event.getId(), event.getEventId(), ex.getMessage(), ex);
                failedCount++;
            }
        }

        Instant completedAt = Instant.now();
        log.info("Reconciliation run {} finished: evaluated={}, MATCHED={}, MISMATCH={}, CORRECTED={}, FAILED={}",
            runId, eligibleEvents.size(), matchedCount, mismatchCount, correctedCount, failedCount);

        return new ReconciliationRunResponse(
            runId,
            startedAt,
            completedAt,
            eligibleEvents.size(),
            matchedCount,
            mismatchCount,
            correctedCount,
            failedCount,
            recordSummaries
        );
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ReconciliationRecord reconcileSingleEvent(WebhookEvent event, boolean allowUpdate) {
        Optional<ReconciliationRecord> existingOpt = reconciliationRecordRepository.findByWebhookEvent(event);
        if (existingOpt.isPresent() && !allowUpdate) {
            log.debug("Event {} already reconciled; skipping reprocessing", event.getEventId());
            return existingOpt.get();
        }

        String internalStatus = event.getStatus().name();
        String externalStatus = null;
        ReconciliationResult result;
        String detailsJson;

        try {
            Optional<ExternalEventRecord> externalRecordOpt = sourceOfTruthClient.fetchExternalRecord(event.getSource(), event.getEventId());

            if (externalRecordOpt.isEmpty()) {
                // External reference missing -> MISMATCH
                externalStatus = "NOT_FOUND";
                result = ReconciliationResult.MISMATCH;
                detailsJson = buildDetailsJson(
                    "MISSING_EXTERNAL_RECORD",
                    "Event not found in external source of truth",
                    internalStatus,
                    externalStatus,
                    event,
                    null
                );
            } else {
                ExternalEventRecord externalRecord = externalRecordOpt.get();
                externalStatus = externalRecord.externalStatus();

                if (isStatusMatching(event.getStatus(), externalStatus)) {
                    result = ReconciliationResult.MATCHED;
                    detailsJson = buildDetailsJson(
                        "NONE",
                        "Internal status matches external reference status",
                        internalStatus,
                        externalStatus,
                        event,
                        externalRecord
                    );
                } else if (isSafeCorrection(externalStatus)) {
                    result = ReconciliationResult.CORRECTED;
                    detailsJson = buildDetailsJson(
                        "AUDIT_CORRECTION",
                        "External provider confirmed settlement; recorded audit correction details without modifying immutable webhook event history",
                        internalStatus,
                        externalStatus,
                        event,
                        externalRecord
                    );
                } else {
                    result = ReconciliationResult.MISMATCH;
                    detailsJson = buildDetailsJson(
                        "STATUS_DISCREPANCY",
                        "Discrepancy detected: internal status does not match external status",
                        internalStatus,
                        externalStatus,
                        event,
                        externalRecord
                    );
                }
            }
        } catch (ExternalProviderException ex) {
            log.warn("External provider error fetching record for eventId={}: {}", event.getEventId(), ex.getMessage());
            result = ReconciliationResult.FAILED;
            externalStatus = null;
            detailsJson = buildDetailsJson(
                "PROVIDER_ERROR",
                "Source-of-truth lookup failed: " + ex.getMessage(),
                internalStatus,
                null,
                event,
                null
            );
        }

        if (existingOpt.isPresent()) {
            ReconciliationRecord existing = existingOpt.get();
            existing.setInternalStatus(internalStatus);
            existing.setExternalStatus(externalStatus);
            existing.setResult(result);
            existing.setReconciledAt(Instant.now());
            existing.setDetails(detailsJson);
            return reconciliationRecordRepository.save(existing);
        } else {
            ReconciliationRecord newRecord = new ReconciliationRecord(
                event,
                externalStatus,
                internalStatus,
                result,
                detailsJson
            );
            try {
                return reconciliationRecordRepository.save(newRecord);
            } catch (DataIntegrityViolationException dive) {
                log.warn("Concurrent duplicate reconciliation detected for eventId={}; returning existing record", event.getEventId());
                return reconciliationRecordRepository.findByWebhookEvent(event)
                    .orElseThrow(() -> dive);
            }
        }
    }

    private boolean isStatusMatching(WebhookStatus internalStatus, String externalStatus) {
        if (externalStatus == null) {
            return false;
        }
        String normalizedExt = externalStatus.trim().toUpperCase(Locale.ROOT);

        if (internalStatus == WebhookStatus.COMPLETED) {
            return MATCHING_SUCCESS_STATUSES.contains(normalizedExt);
        }
        if (internalStatus == WebhookStatus.DEAD_LETTER || internalStatus == WebhookStatus.FAILED) {
            return MATCHING_FAILURE_STATUSES.contains(normalizedExt);
        }
        return false;
    }

    private boolean isSafeCorrection(String externalStatus) {
        if (externalStatus == null) {
            return false;
        }
        return SAFE_CORRECTION_STATUSES.contains(externalStatus.trim().toUpperCase(Locale.ROOT));
    }

    private String buildDetailsJson(
        String discrepancyType,
        String reason,
        String internalStatus,
        String externalStatus,
        WebhookEvent event,
        ExternalEventRecord externalRecord
    ) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("discrepancyType", discrepancyType);
            root.put("reason", reason);
            root.put("internalStatus", internalStatus);
            if (externalStatus != null) {
                root.put("externalStatus", externalStatus);
            } else {
                root.putNull("externalStatus");
            }
            root.put("eventId", event.getEventId());
            root.put("source", event.getSource());
            root.put("eventType", event.getEventType());
            root.put("reconciledAt", Instant.now().toString());

            if (externalRecord != null && externalRecord.details() != null && !externalRecord.details().isEmpty()) {
                root.set("externalMetadata", objectMapper.valueToTree(externalRecord.details()));
            }

            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            return "{\"reason\": \"" + reason + "\"}";
        }
    }

    @Override
    @Transactional(readOnly = true)
    public Page<ReconciliationRecordSummaryResponse> listRecords(ReconciliationResult result, String source, Pageable pageable) {
        Page<ReconciliationRecord> page;
        if (result != null && source != null && !source.isBlank()) {
            page = reconciliationRecordRepository.findByResultAndWebhookEventSource(result, source.trim(), pageable);
        } else if (result != null) {
            page = reconciliationRecordRepository.findByResult(result, pageable);
        } else if (source != null && !source.isBlank()) {
            page = reconciliationRecordRepository.findByWebhookEventSource(source.trim(), pageable);
        } else {
            page = reconciliationRecordRepository.findAll(pageable);
        }
        return page.map(ReconciliationRecordSummaryResponse::fromEntity);
    }

    @Override
    @Transactional(readOnly = true)
    public ReconciliationRecordDetailResponse getRecord(UUID id) {
        ReconciliationRecord record = reconciliationRecordRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Reconciliation record not found with ID: " + id));

        return ReconciliationRecordDetailResponse.fromEntity(record, objectMapper);
    }
}
