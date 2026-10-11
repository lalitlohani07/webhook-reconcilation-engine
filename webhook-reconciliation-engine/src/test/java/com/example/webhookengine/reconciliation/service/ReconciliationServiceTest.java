package com.example.webhookengine.reconciliation.service;

import com.example.webhookengine.controller.dto.ReconciliationRecordDetailResponse;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReconciliationServiceTest {

    @Mock
    private WebhookEventRepository webhookEventRepository;

    @Mock
    private ReconciliationRecordRepository reconciliationRecordRepository;

    @Mock
    private SourceOfTruthClient sourceOfTruthClient;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ReconciliationProperties reconciliationProperties;
    private ReconciliationServiceImpl reconciliationService;

    @BeforeEach
    void setUp() {
        reconciliationProperties = new ReconciliationProperties();
        reconciliationProperties.setDefaultBatchSize(50);
        reconciliationProperties.setMaxBatchSize(100);

        reconciliationService = new ReconciliationServiceImpl(
            webhookEventRepository,
            reconciliationRecordRepository,
            sourceOfTruthClient,
            reconciliationProperties,
            objectMapper
        );
    }

    private WebhookEvent createEvent(UUID id, String eventId, String source, WebhookStatus status) {
        WebhookEvent event = new WebhookEvent(
            eventId,
            source,
            "payment.captured",
            "{\"id\":\"" + eventId + "\",\"amount\":100.00}",
            status
        );
        event.setId(id);
        return event;
    }

    @Test
    @DisplayName("Should produce MATCHED when local COMPLETED matches external PAID")
    void testReconcile_MatchedCompleted() throws Exception {
        UUID id = UUID.randomUUID();
        WebhookEvent event = createEvent(id, "evt-123", "stripe", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event));
        when(reconciliationRecordRepository.findByWebhookEvent(event))
            .thenReturn(Optional.empty());

        ExternalEventRecord externalRecord = new ExternalEventRecord(
            "evt-123",
            "stripe",
            "PAID",
            "payment.captured",
            Instant.now(),
            Map.of("chargeId", "ch_123")
        );
        when(sourceOfTruthClient.fetchExternalRecord("stripe", "evt-123"))
            .thenReturn(Optional.of(externalRecord));

        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse runResponse = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        assertThat(runResponse.totalEvaluated()).isEqualTo(1);
        assertThat(runResponse.matchedCount()).isEqualTo(1);
        assertThat(runResponse.mismatchCount()).isZero();
        assertThat(runResponse.correctedCount()).isZero();
        assertThat(runResponse.failedCount()).isZero();

        ArgumentCaptor<ReconciliationRecord> captor = ArgumentCaptor.forClass(ReconciliationRecord.class);
        verify(reconciliationRecordRepository).save(captor.capture());
        ReconciliationRecord saved = captor.getValue();
        assertThat(saved.getResult()).isEqualTo(ReconciliationResult.MATCHED);
        assertThat(saved.getWebhookEvent()).isEqualTo(event);
        assertThat(saved.getExternalStatus()).isEqualTo("PAID");
        assertThat(saved.getInternalStatus()).isEqualTo("COMPLETED");
        assertThat(saved.getDetails()).contains("\"discrepancyType\":\"NONE\"");
    }

    @Test
    @DisplayName("Should produce MATCHED when local DEAD_LETTER matches external FAILED")
    void testReconcile_MatchedDeadLetter() throws Exception {
        UUID id = UUID.randomUUID();
        WebhookEvent event = createEvent(id, "evt-fail", "adyen", WebhookStatus.DEAD_LETTER);

        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event));
        when(reconciliationRecordRepository.findByWebhookEvent(event))
            .thenReturn(Optional.empty());

        ExternalEventRecord externalRecord = new ExternalEventRecord(
            "evt-fail",
            "adyen",
            "FAILED",
            "payment.failed",
            Instant.now(),
            Map.of("reason", "insufficient_funds")
        );
        when(sourceOfTruthClient.fetchExternalRecord("adyen", "evt-fail"))
            .thenReturn(Optional.of(externalRecord));
        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest());

        assertThat(response.matchedCount()).isEqualTo(1);
        ArgumentCaptor<ReconciliationRecord> captor = ArgumentCaptor.forClass(ReconciliationRecord.class);
        verify(reconciliationRecordRepository).save(captor.capture());
        assertThat(captor.getValue().getResult()).isEqualTo(ReconciliationResult.MATCHED);
    }

    @Test
    @DisplayName("Should produce CORRECTED when external reference is settled with adjustment")
    void testReconcile_CorrectedExternalAdjustment() throws Exception {
        UUID id = UUID.randomUUID();
        WebhookEvent event = createEvent(id, "evt-adj", "stripe", WebhookStatus.DEAD_LETTER);

        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event));
        when(reconciliationRecordRepository.findByWebhookEvent(event))
            .thenReturn(Optional.empty());

        ExternalEventRecord externalRecord = new ExternalEventRecord(
            "evt-adj",
            "stripe",
            "SETTLED_WITH_ADJUSTMENT",
            "adjustment",
            Instant.now(),
            Map.of("adjustmentType", "partial_refund")
        );
        when(sourceOfTruthClient.fetchExternalRecord("stripe", "evt-adj"))
            .thenReturn(Optional.of(externalRecord));
        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        assertThat(response.correctedCount()).isEqualTo(1);
        ArgumentCaptor<ReconciliationRecord> captor = ArgumentCaptor.forClass(ReconciliationRecord.class);
        verify(reconciliationRecordRepository).save(captor.capture());
        ReconciliationRecord saved = captor.getValue();
        assertThat(saved.getResult()).isEqualTo(ReconciliationResult.CORRECTED);
        assertThat(saved.getDetails()).contains("\"discrepancyType\":\"AUDIT_CORRECTION\"");
        assertThat(saved.getDetails()).contains("SETTLED_WITH_ADJUSTMENT");
    }

    @Test
    @DisplayName("Should produce MISMATCH when status contradicts external source")
    void testReconcile_MismatchStatusContradiction() throws Exception {
        UUID id = UUID.randomUUID();
        WebhookEvent event = createEvent(id, "evt-mis", "stripe", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event));
        when(reconciliationRecordRepository.findByWebhookEvent(event))
            .thenReturn(Optional.empty());

        ExternalEventRecord externalRecord = new ExternalEventRecord(
            "evt-mis",
            "stripe",
            "FAILED",
            "payment.failed",
            Instant.now(),
            Map.of("error", "card_declined")
        );
        when(sourceOfTruthClient.fetchExternalRecord("stripe", "evt-mis"))
            .thenReturn(Optional.of(externalRecord));
        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        assertThat(response.mismatchCount()).isEqualTo(1);
        ArgumentCaptor<ReconciliationRecord> captor = ArgumentCaptor.forClass(ReconciliationRecord.class);
        verify(reconciliationRecordRepository).save(captor.capture());
        ReconciliationRecord saved = captor.getValue();
        assertThat(saved.getResult()).isEqualTo(ReconciliationResult.MISMATCH);
        assertThat(saved.getDetails()).contains("\"discrepancyType\":\"STATUS_DISCREPANCY\"");
    }

    @Test
    @DisplayName("Should produce MISMATCH when external reference record is missing (not found)")
    void testReconcile_MismatchRecordNotFound() throws Exception {
        UUID id = UUID.randomUUID();
        WebhookEvent event = createEvent(id, "evt-404", "paypal", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event));
        when(reconciliationRecordRepository.findByWebhookEvent(event))
            .thenReturn(Optional.empty());

        when(sourceOfTruthClient.fetchExternalRecord("paypal", "evt-404"))
            .thenReturn(Optional.empty());
        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        assertThat(response.mismatchCount()).isEqualTo(1);
        ArgumentCaptor<ReconciliationRecord> captor = ArgumentCaptor.forClass(ReconciliationRecord.class);
        verify(reconciliationRecordRepository).save(captor.capture());
        ReconciliationRecord saved = captor.getValue();
        assertThat(saved.getResult()).isEqualTo(ReconciliationResult.MISMATCH);
        assertThat(saved.getExternalStatus()).isEqualTo("NOT_FOUND");
        assertThat(saved.getDetails()).contains("\"discrepancyType\":\"MISSING_EXTERNAL_RECORD\"");
    }

    @Test
    @DisplayName("Should produce FAILED when provider throws ExternalProviderException without aborting batch")
    void testReconcile_ProviderErrorRecordsFailedWithoutAbortingBatch() throws Exception {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        WebhookEvent event1 = createEvent(id1, "evt-err", "stripe", WebhookStatus.COMPLETED);
        WebhookEvent event2 = createEvent(id2, "evt-ok", "stripe", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event1, event2));
        when(reconciliationRecordRepository.findByWebhookEvent(event1))
            .thenReturn(Optional.empty());
        when(reconciliationRecordRepository.findByWebhookEvent(event2))
            .thenReturn(Optional.empty());

        when(sourceOfTruthClient.fetchExternalRecord("stripe", "evt-err"))
            .thenThrow(new ExternalProviderException("Simulated network timeout connecting to provider"));

        ExternalEventRecord okRecord = new ExternalEventRecord(
            "evt-ok", "stripe", "COMPLETED", "payment", Instant.now(), Collections.emptyMap()
        );
        when(sourceOfTruthClient.fetchExternalRecord("stripe", "evt-ok"))
            .thenReturn(Optional.of(okRecord));

        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        assertThat(response.totalEvaluated()).isEqualTo(2);
        assertThat(response.failedCount()).isEqualTo(1);
        assertThat(response.matchedCount()).isEqualTo(1);

        verify(reconciliationRecordRepository, times(2)).save(any(ReconciliationRecord.class));
    }

    @Test
    @DisplayName("Should update existing record and not duplicate when event is re-reconciled")
    void testReconcile_ReprocessingUpdatesExistingRecord() throws Exception {
        UUID id = UUID.randomUUID();
        WebhookEvent event = createEvent(id, "evt-dup", "stripe", WebhookStatus.COMPLETED);

        when(webhookEventRepository.findEligibleEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of(event));

        ReconciliationRecord existing = new ReconciliationRecord(
            UUID.randomUUID(),
            event,
            "ERROR",
            "COMPLETED",
            ReconciliationResult.FAILED,
            Instant.now().minusSeconds(3600),
            "{}"
        );

        when(reconciliationRecordRepository.findByWebhookEvent(event))
            .thenReturn(Optional.of(existing));

        ExternalEventRecord externalRecord = new ExternalEventRecord(
            "evt-dup", "stripe", "PAID"
        );
        when(sourceOfTruthClient.fetchExternalRecord("stripe", "evt-dup"))
            .thenReturn(Optional.of(externalRecord));
        when(reconciliationRecordRepository.save(any(ReconciliationRecord.class)))
            .thenAnswer(inv -> inv.getArgument(0));

        ReconciliationRunResponse response = reconciliationService.runReconciliation(
            new ReconciliationRunRequest(10, null, null, true)
        );

        assertThat(response.matchedCount()).isEqualTo(1);
        ArgumentCaptor<ReconciliationRecord> captor = ArgumentCaptor.forClass(ReconciliationRecord.class);
        verify(reconciliationRecordRepository).save(captor.capture());
        ReconciliationRecord updated = captor.getValue();
        assertThat(updated.getId()).isEqualTo(existing.getId());
        assertThat(updated.getResult()).isEqualTo(ReconciliationResult.MATCHED);
    }

    @Test
    @DisplayName("Should clamp batch size to configured maxBatchSize")
    void testReconcile_EnforcesMaxBatchSize() {
        when(webhookEventRepository.findEligibleUnreconciledEvents(anyList(), any(), any(Pageable.class)))
            .thenReturn(List.of());

        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest(9999));

        assertThat(response.totalEvaluated()).isZero();
        ArgumentCaptor<Pageable> captor = ArgumentCaptor.forClass(Pageable.class);
        verify(webhookEventRepository).findEligibleUnreconciledEvents(anyList(), any(), captor.capture());
        assertThat(captor.getValue().getPageSize()).isEqualTo(reconciliationProperties.getMaxBatchSize());
    }

    @Test
    @DisplayName("Should retrieve reconciliation record details by ID")
    void testGetRecord_Success() {
        UUID recordId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        WebhookEvent event = createEvent(eventId, "evt-inspect", "stripe", WebhookStatus.COMPLETED);
        String detailsJson = "{\"discrepancyType\":\"NONE\",\"internalStatus\":\"COMPLETED\",\"externalStatus\":\"PAID\"}";

        ReconciliationRecord record = new ReconciliationRecord(
            recordId,
            event,
            "PAID",
            "COMPLETED",
            ReconciliationResult.MATCHED,
            Instant.now(),
            detailsJson
        );

        when(reconciliationRecordRepository.findById(recordId)).thenReturn(Optional.of(record));

        ReconciliationRecordDetailResponse detail = reconciliationService.getRecord(recordId);

        assertThat(detail.id()).isEqualTo(recordId);
        assertThat(detail.webhookEventId()).isEqualTo(eventId);
        assertThat(detail.eventId()).isEqualTo("evt-inspect");
        assertThat(detail.source()).isEqualTo("stripe");
        assertThat(detail.result()).isEqualTo(ReconciliationResult.MATCHED);
        assertThat(detail.details().get("discrepancyType").asText()).isEqualTo("NONE");
        assertThat(detail.details().get("internalStatus").asText()).isEqualTo("COMPLETED");
        assertThat(detail.details().get("externalStatus").asText()).isEqualTo("PAID");
    }

    @Test
    @DisplayName("Should throw ResourceNotFoundException when record ID does not exist")
    void testGetRecord_NotFound() {
        UUID nonExistentId = UUID.randomUUID();
        when(reconciliationRecordRepository.findById(nonExistentId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> reconciliationService.getRecord(nonExistentId))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("Reconciliation record not found with ID: " + nonExistentId);
    }
}
