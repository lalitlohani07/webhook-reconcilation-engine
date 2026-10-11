package com.example.webhookengine.integration;

import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.controller.dto.ReconciliationRecordDetailResponse;
import com.example.webhookengine.controller.dto.ReconciliationRunRequest;
import com.example.webhookengine.controller.dto.ReconciliationRunResponse;
import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.reconciliation.config.ReconciliationProperties;
import com.example.webhookengine.reconciliation.provider.SimulatedSourceOfTruthClient;
import com.example.webhookengine.reconciliation.service.ReconciliationServiceImpl;
import com.example.webhookengine.repository.ReconciliationRecordRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({
    TestFlywayConfig.class,
    ReconciliationProperties.class,
    SimulatedSourceOfTruthClient.class,
    ReconciliationServiceImpl.class,
    ObjectMapper.class
})
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:reconntestdb;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver"
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class ReconciliationIntegrationTest {

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private ReconciliationRecordRepository reconciliationRecordRepository;

    @Autowired
    private SimulatedSourceOfTruthClient simulatedSourceOfTruthClient;

    @Autowired
    private ReconciliationServiceImpl reconciliationService;

    @BeforeEach
    void setUp() {
        simulatedSourceOfTruthClient.clearOverrides();
        reconciliationRecordRepository.deleteAll();
        webhookEventRepository.deleteAll();
    }

    private WebhookEvent createAndSaveEvent(String eventId, String source, WebhookStatus status) {
        WebhookEvent event = new WebhookEvent(
            eventId,
            source,
            "payment.updated",
            "{\"id\":\"" + eventId + "\"}",
            status
        );
        return webhookEventRepository.saveAndFlush(event);
    }

    @Test
    @DisplayName("End-to-end reconciliation run classifies events and persists records in database")
    void shouldReconcileBatchEndToEnd() {
        // 1. MATCHED: Completed event matches default simulated behavior (PAID)
        createAndSaveEvent("evt_matched", "stripe", WebhookStatus.COMPLETED);

        // 2. MISMATCH: Completed event, but external record simulated as not found via '_notfound' suffix
        createAndSaveEvent("evt_missing_notfound", "stripe", WebhookStatus.COMPLETED);

        // 3. CORRECTED: Dead-letter event, external record simulated with '_corrected' suffix
        createAndSaveEvent("evt_settled_corrected", "stripe", WebhookStatus.DEAD_LETTER);

        // 4. FAILED: Completed event, simulated network error via '_error' suffix
        createAndSaveEvent("evt_provider_error", "stripe", WebhookStatus.COMPLETED);

        // Execute reconciliation batch
        ReconciliationRunResponse response = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        assertThat(response.totalEvaluated()).isEqualTo(4);
        assertThat(response.matchedCount()).isEqualTo(1);
        assertThat(response.mismatchCount()).isEqualTo(1);
        assertThat(response.correctedCount()).isEqualTo(1);
        assertThat(response.failedCount()).isEqualTo(1);

        // Verify DB persistence
        List<ReconciliationRecord> records = reconciliationRecordRepository.findAll();
        assertThat(records).hasSize(4);

        // Inspect detail of MATCHED record
        ReconciliationRecord matchedRecord = records.stream()
            .filter(r -> r.getResult() == ReconciliationResult.MATCHED)
            .findFirst()
            .orElseThrow();
        ReconciliationRecordDetailResponse detail = reconciliationService.getRecord(matchedRecord.getId());
        assertThat(detail.eventId()).isEqualTo("evt_matched");
        assertThat(detail.result()).isEqualTo(ReconciliationResult.MATCHED);
        assertThat(detail.details().get("discrepancyType").asText()).isEqualTo("NONE");
        assertThat(detail.details().get("internalStatus").asText()).isEqualTo("COMPLETED");

        // Inspect detail of CORRECTED record
        ReconciliationRecord correctedRecord = records.stream()
            .filter(r -> r.getResult() == ReconciliationResult.CORRECTED)
            .findFirst()
            .orElseThrow();
        assertThat(correctedRecord.getDetails()).contains("SETTLED_WITH_ADJUSTMENT");

        // Inspect detail of FAILED record
        ReconciliationRecord failedRecord = records.stream()
            .filter(r -> r.getResult() == ReconciliationResult.FAILED)
            .findFirst()
            .orElseThrow();
        assertThat(failedRecord.getDetails()).contains("Simulated upstream provider connection timeout");
    }

    @Test
    @DisplayName("Pagination and filtering by result and source work on repository")
    void shouldFilterAndPaginateReconciliationRecords() {
        WebhookEvent stripeMatched = createAndSaveEvent("evt_stripe_m", "stripe", WebhookStatus.COMPLETED);
        WebhookEvent adyenMatched = createAndSaveEvent("evt_adyen_m", "adyen", WebhookStatus.COMPLETED);
        WebhookEvent stripeMismatch = createAndSaveEvent("evt_stripe_fail_mismatch", "stripe", WebhookStatus.COMPLETED);

        reconciliationService.runReconciliation(new ReconciliationRunRequest(10));

        // Filter by MATCHED
        Page<ReconciliationRecord> matchedPage = reconciliationRecordRepository.findByResult(
            ReconciliationResult.MATCHED,
            PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "reconciledAt"))
        );
        assertThat(matchedPage.getTotalElements()).isEqualTo(2);

        // Filter by source adyen
        Page<ReconciliationRecord> adyenPage = reconciliationRecordRepository.findByWebhookEventSource(
            "adyen",
            PageRequest.of(0, 10)
        );
        assertThat(adyenPage.getTotalElements()).isEqualTo(1);
        assertThat(adyenPage.getContent().get(0).getWebhookEvent().getSource()).isEqualTo("adyen");

        // Filter by result MISMATCH and source stripe
        Page<ReconciliationRecord> filteredPage = reconciliationRecordRepository.findByResultAndWebhookEventSource(
            ReconciliationResult.MISMATCH,
            "stripe",
            PageRequest.of(0, 10)
        );
        assertThat(filteredPage.getTotalElements()).isEqualTo(1);
        assertThat(filteredPage.getContent().get(0).getResult()).isEqualTo(ReconciliationResult.MISMATCH);
    }

    @Test
    @DisplayName("Reprocessing previously reconciled events does not create duplicate records")
    void shouldNotCreateDuplicatesOnReconciliationRerun() {
        WebhookEvent event = createAndSaveEvent("evt_rerun", "stripe", WebhookStatus.COMPLETED);

        // First run
        ReconciliationRunResponse firstRun = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));
        assertThat(firstRun.totalEvaluated()).isEqualTo(1);
        assertThat(reconciliationRecordRepository.count()).isEqualTo(1);

        // Re-run
        ReconciliationRunResponse secondRun = reconciliationService.runReconciliation(new ReconciliationRunRequest(10));
        assertThat(secondRun.totalEvaluated()).isZero(); // Already reconciled events are excluded from unreconciled scan
        assertThat(reconciliationRecordRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Concurrent reconciliation on the same event does not create duplicate records")
    void shouldHandleConcurrentReconciliationWithoutDuplicates() throws Exception {
        WebhookEvent event = createAndSaveEvent("evt_concurrent", "stripe", WebhookStatus.COMPLETED);

        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger processedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    reconciliationService.reconcileSingleEvent(event, false);
                    processedCount.incrementAndGet();
                } catch (Throwable t) {
                    // Ignored or logged
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        boolean completed = doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        // Database unique constraint uk_reconciliation_records_event_id guarantees only 1 record exists!
        List<ReconciliationRecord> records = reconciliationRecordRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getWebhookEvent().getId()).isEqualTo(event.getId());
    }
}
