package com.example.webhookengine.repository;

import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.config.TestFlywayConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@Import(TestFlywayConfig.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:testdb;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver"
})
class RepositoryPersistenceTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private WebhookAttemptRepository webhookAttemptRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ReconciliationRecordRepository reconciliationRecordRepository;

    // --- WebhookEvent Tests ---

    @Test
    @DisplayName("WebhookEvent: save and find by ID")
    void shouldSaveAndFindWebhookEventById() {
        WebhookEvent event = new WebhookEvent(
            "evt_101",
            "stripe",
            "payment_intent.succeeded",
            "{\"amount\": 5000}",
            WebhookStatus.RECEIVED
        );

        WebhookEvent saved = webhookEventRepository.save(event);
        entityManager.flush();
        entityManager.clear();

        Optional<WebhookEvent> found = webhookEventRepository.findById(saved.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getEventId()).isEqualTo("evt_101");
        assertThat(found.get().getSource()).isEqualTo("stripe");
        assertThat(found.get().getEventType()).isEqualTo("payment_intent.succeeded");
        assertThat(found.get().getPayload()).contains("5000");
        assertThat(found.get().getStatus()).isEqualTo(WebhookStatus.RECEIVED);
        assertThat(found.get().getCreatedAt()).isNotNull();
        assertThat(found.get().getUpdatedAt()).isNotNull();
    }

    @Test
    @DisplayName("WebhookEvent: find by source and event ID")
    void shouldFindBySourceAndEventId() {
        WebhookEvent event = new WebhookEvent(
            "evt_202",
            "github",
            "push",
            "{\"ref\": \"refs/heads/main\"}",
            WebhookStatus.RECEIVED
        );
        webhookEventRepository.save(event);
        entityManager.flush();
        entityManager.clear();

        Optional<WebhookEvent> found = webhookEventRepository.findBySourceAndEventId("github", "evt_202");
        assertThat(found).isPresent();
        assertThat(found.get().getEventType()).isEqualTo("push");
    }

    @Test
    @DisplayName("WebhookEvent: duplicate (source, eventId) is rejected")
    void shouldRejectDuplicateSourceAndEventId() {
        WebhookEvent event1 = new WebhookEvent("evt_dup", "stripe", "payment.created", "{}", WebhookStatus.RECEIVED);
        webhookEventRepository.save(event1);
        entityManager.flush();

        WebhookEvent event2 = new WebhookEvent("evt_dup", "stripe", "payment.updated", "{}", WebhookStatus.RECEIVED);
        webhookEventRepository.save(event2);

        assertThatThrownBy(() -> entityManager.flush())
            .hasRootCauseInstanceOf(org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException.class);
    }

    @Test
    @DisplayName("WebhookEvent: same event ID from different sources is allowed")
    void shouldAllowSameEventIdFromDifferentSources() {
        WebhookEvent stripeEvent = new WebhookEvent("evt_shared", "stripe", "charge.succeeded", "{}", WebhookStatus.RECEIVED);
        WebhookEvent githubEvent = new WebhookEvent("evt_shared", "github", "workflow_run", "{}", WebhookStatus.RECEIVED);

        webhookEventRepository.save(stripeEvent);
        webhookEventRepository.save(githubEvent);
        entityManager.flush();
        entityManager.clear();

        assertThat(webhookEventRepository.findBySourceAndEventId("stripe", "evt_shared")).isPresent();
        assertThat(webhookEventRepository.findBySourceAndEventId("github", "evt_shared")).isPresent();
    }

    @Test
    @DisplayName("WebhookEvent: findDueEventsForRetry returns only RETRY_PENDING events with nextRetryAt <= threshold")
    void shouldFindDueEventsForRetry() {
        Instant now = Instant.now();

        // 1. Due event: RETRY_PENDING, nextRetryAt in past
        WebhookEvent due = new WebhookEvent("evt_due", "stripe", "charge.failed", "{}", WebhookStatus.RETRY_PENDING);
        due.setNextRetryAt(now.minusSeconds(30));
        webhookEventRepository.save(due);

        // 2. Future event: RETRY_PENDING, nextRetryAt in future
        WebhookEvent future = new WebhookEvent("evt_future", "stripe", "charge.failed", "{}", WebhookStatus.RETRY_PENDING);
        future.setNextRetryAt(now.plusSeconds(300));
        webhookEventRepository.save(future);

        // 3. Different status: FAILED, nextRetryAt in past
        WebhookEvent failed = new WebhookEvent("evt_failed", "stripe", "charge.failed", "{}", WebhookStatus.FAILED);
        failed.setNextRetryAt(now.minusSeconds(30));
        webhookEventRepository.save(failed);

        entityManager.flush();
        entityManager.clear();

        List<WebhookEvent> dueList = webhookEventRepository.findDueEventsForRetry(
            WebhookStatus.RETRY_PENDING,
            now,
            org.springframework.data.domain.PageRequest.of(0, 10)
        );

        assertThat(dueList).hasSize(1);
        assertThat(dueList.get(0).getEventId()).isEqualTo("evt_due");
    }

    @Test
    @DisplayName("WebhookEvent: claimEventForRetry atomically updates status and returns 1 on success, 0 on race")
    void shouldClaimEventForRetryAtomically() {
        Instant now = Instant.now();
        WebhookEvent event = new WebhookEvent("evt_claim", "stripe", "charge.failed", "{}", WebhookStatus.RETRY_PENDING);
        event.setNextRetryAt(now.minusSeconds(10));
        WebhookEvent saved = webhookEventRepository.save(event);
        entityManager.flush();
        entityManager.clear();

        // First claim succeeds
        int updated = webhookEventRepository.claimEventForRetry(saved.getId(), WebhookStatus.RETRY_PENDING, WebhookStatus.RECEIVED, now);
        assertThat(updated).isEqualTo(1);

        // Second claim fails because status is now RECEIVED
        int secondUpdate = webhookEventRepository.claimEventForRetry(saved.getId(), WebhookStatus.RETRY_PENDING, WebhookStatus.RECEIVED, now);
        assertThat(secondUpdate).isEqualTo(0);
    }

    // --- WebhookAttempt Tests ---

    @Test
    @DisplayName("WebhookAttempt: save and retrieve attempts for event")
    void shouldSaveAndRetrieveAttemptsForEvent() {
        WebhookEvent event = new WebhookEvent("evt_att", "stripe", "invoice.paid", "{}", WebhookStatus.PROCESSING);
        webhookEventRepository.save(event);

        WebhookAttempt attempt1 = new WebhookAttempt(event, 1, WebhookStatus.FAILED);
        attempt1.setErrorMessage("Network timeout");
        attempt1.setCompletedAt(Instant.now());
        webhookAttemptRepository.save(attempt1);

        WebhookAttempt attempt2 = new WebhookAttempt(event, 2, WebhookStatus.COMPLETED);
        attempt2.setCompletedAt(Instant.now());
        webhookAttemptRepository.save(attempt2);

        entityManager.flush();
        entityManager.clear();

        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(event);
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0).getAttemptNumber()).isEqualTo(1);
        assertThat(attempts.get(0).getErrorMessage()).isEqualTo("Network timeout");
        assertThat(attempts.get(1).getAttemptNumber()).isEqualTo(2);
        assertThat(attempts.get(1).getStatus()).isEqualTo(WebhookStatus.COMPLETED);
    }

    @Test
    @DisplayName("WebhookAttempt: duplicate attempt number for same event is rejected")
    void shouldRejectDuplicateAttemptNumberForSameEvent() {
        WebhookEvent event = new WebhookEvent("evt_att_dup", "stripe", "charge.refunded", "{}", WebhookStatus.PROCESSING);
        webhookEventRepository.save(event);

        WebhookAttempt attempt1 = new WebhookAttempt(event, 1, WebhookStatus.FAILED);
        webhookAttemptRepository.save(attempt1);
        entityManager.flush();

        WebhookAttempt attempt2 = new WebhookAttempt(event, 1, WebhookStatus.COMPLETED);
        webhookAttemptRepository.save(attempt2);

        assertThatThrownBy(() -> entityManager.flush())
            .hasRootCauseInstanceOf(org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException.class);
    }

    // --- OutboxEvent Tests ---

    @Test
    @DisplayName("OutboxEvent: save and retrieve pending events")
    void shouldSaveAndRetrievePendingOutboxEvents() {
        OutboxEvent outbox1 = new OutboxEvent("agg_1", "payment.processed", "{\"id\": 1}", OutboxStatus.PENDING);
        OutboxEvent outbox2 = new OutboxEvent("agg_2", "payment.processed", "{\"id\": 2}", OutboxStatus.PENDING);
        OutboxEvent outbox3 = new OutboxEvent("agg_3", "payment.processed", "{\"id\": 3}", OutboxStatus.PUBLISHED);

        outboxEventRepository.save(outbox1);
        outboxEventRepository.save(outbox2);
        outboxEventRepository.save(outbox3);
        entityManager.flush();
        entityManager.clear();

        List<OutboxEvent> pending = outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);
        assertThat(pending).hasSize(2);
        assertThat(pending).extracting(OutboxEvent::getAggregateId).containsExactly("agg_1", "agg_2");
    }

    // --- ReconciliationRecord Tests ---

    @Test
    @DisplayName("ReconciliationRecord: save and retrieve by webhook event")
    void shouldSaveAndRetrieveReconciliationRecordByWebhookEvent() {
        WebhookEvent event = new WebhookEvent("evt_rec", "shopify", "order.created", "{}", WebhookStatus.COMPLETED);
        webhookEventRepository.save(event);

        ReconciliationRecord record = new ReconciliationRecord(
            event,
            "DELIVERED",
            "COMPLETED",
            ReconciliationResult.MATCHED,
            "{\"checked\": true}"
        );
        reconciliationRecordRepository.save(record);
        entityManager.flush();
        entityManager.clear();

        List<ReconciliationRecord> records = reconciliationRecordRepository.findByWebhookEventOrderByReconciledAtDesc(event);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getResult()).isEqualTo(ReconciliationResult.MATCHED);
        assertThat(records.get(0).getExternalStatus()).isEqualTo("DELIVERED");
        assertThat(records.get(0).getInternalStatus()).isEqualTo("COMPLETED");
        assertThat(records.get(0).getDetails()).contains("checked");
    }
}
