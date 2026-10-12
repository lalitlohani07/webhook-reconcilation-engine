package com.example.webhookengine.integration;

import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookReplay;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.InvalidStateTransitionException;
import com.example.webhookengine.exception.RateLimiterUnavailableException;
import com.example.webhookengine.messaging.OutboxPublisher;
import com.example.webhookengine.messaging.WebhookEventPublisher;
import com.example.webhookengine.messaging.WebhookPublishingException;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import com.example.webhookengine.ratelimit.service.RateLimiterService;
import com.example.webhookengine.reconciliation.provider.SimulatedSourceOfTruthClient;
import com.example.webhookengine.reconciliation.service.ReconciliationServiceImpl;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.ReconciliationRecordRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.repository.WebhookReplayRepository;
import com.example.webhookengine.security.domain.SecurityRole;
import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import com.example.webhookengine.service.DeadLetterService;
import com.example.webhookengine.service.WebhookEventProcessor;
import com.example.webhookengine.service.WebhookProcessingService;
import com.example.webhookengine.service.WebhookRetryScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestFlywayConfig.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:failure_scenarios_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "management.health.redis.enabled=false",
    "management.health.rabbit.enabled=false",
    "app.outbox.enabled=true",
    "app.outbox.polling-interval-ms=3600000",
    "app.retry.enabled=true",
    "app.retry.scheduler-interval-ms=3600000",
    "app.retry.max-attempts=3",
    "app.retry.initial-delay-ms=1000",
    "app.retry.multiplier=2.0",
    "app.retry.max-delay-ms=10000"
})
public class ReliabilityFailureScenariosIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private WebhookAttemptRepository webhookAttemptRepository;

    @Autowired
    private WebhookReplayRepository webhookReplayRepository;

    @Autowired
    private ReconciliationRecordRepository reconciliationRecordRepository;

    @Autowired
    private SecurityUserRepository securityUserRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private WebhookProcessingService webhookProcessingService;

    @Autowired
    private WebhookRetryScheduler webhookRetryScheduler;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private ReconciliationServiceImpl reconciliationService;

    @Autowired
    private SimulatedSourceOfTruthClient simulatedSourceOfTruthClient;

    @MockitoBean
    private RateLimiterService rateLimiterService;

    @MockitoBean
    private WebhookEventPublisher webhookEventPublisher;

    @MockitoSpyBean
    private WebhookEventProcessor webhookEventProcessor;

    @MockitoBean
    private org.springframework.amqp.rabbit.connection.ConnectionFactory rabbitConnectionFactory;

    @MockitoBean
    private org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory redisConnectionFactory;

    private String adminAuthHeader;
    private String viewerAuthHeader;

    @BeforeEach
    void setUp() {
        reconciliationRecordRepository.deleteAll();
        webhookReplayRepository.deleteAll();
        webhookAttemptRepository.deleteAll();
        outboxEventRepository.deleteAll();
        webhookEventRepository.deleteAll();
        securityUserRepository.deleteAll();
        simulatedSourceOfTruthClient.clearOverrides();

        when(rateLimiterService.tryAcquire(any())).thenReturn(RateLimitResult.allowed(60, 59));

        SecurityUser admin = new SecurityUser(
            UUID.randomUUID(),
            "admin_user",
            passwordEncoder.encode("admin_pass"),
            SecurityRole.ADMIN,
            true
        );
        SecurityUser viewer = new SecurityUser(
            UUID.randomUUID(),
            "viewer_user",
            passwordEncoder.encode("viewer_pass"),
            SecurityRole.VIEWER,
            true
        );
        securityUserRepository.saveAndFlush(admin);
        securityUserRepository.saveAndFlush(viewer);

        adminAuthHeader = "Basic " + Base64.getEncoder().encodeToString("admin_user:admin_pass".getBytes(StandardCharsets.UTF_8));
        viewerAuthHeader = "Basic " + Base64.getEncoder().encodeToString("viewer_user:viewer_pass".getBytes(StandardCharsets.UTF_8));
    }

    // =========================================================================
    // 1. RabbitMQ unavailable during publishing
    // =========================================================================
    @Test
    @DisplayName("Scenario 1: RabbitMQ unavailable during publishing leaves record PENDING and increments attemptCount")
    void testRabbitMqUnavailableDuringPublishing() {
        OutboxEvent outboxEvent = new OutboxEvent(
            UUID.randomUUID().toString(),
            "payment.succeeded",
            "{\"webhookEventId\":\"" + UUID.randomUUID() + "\",\"eventId\":\"evt_fail_1\",\"source\":\"stripe\",\"eventType\":\"payment.succeeded\"}",
            OutboxStatus.PENDING
        );
        outboxEvent = outboxEventRepository.saveAndFlush(outboxEvent);

        doThrow(new WebhookPublishingException("RabbitMQ connection refused: connection reset by peer"))
            .when(webhookEventPublisher).publishProcessingMessage(any());

        outboxPublisher.publishSingleEvent(outboxEvent);

        OutboxEvent afterAttempt = outboxEventRepository.findById(outboxEvent.getId()).orElseThrow();
        assertThat(afterAttempt.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(afterAttempt.getAttemptCount()).isEqualTo(1);
        assertThat(afterAttempt.getLastError()).contains("RabbitMQ connection refused");
        assertThat(afterAttempt.getPublishedAt()).isNull();
    }

    // =========================================================================
    // 2. Publisher confirmation failure or timeout
    // =========================================================================
    @Test
    @DisplayName("Scenario 2: Publisher confirmation failure or timeout leaves record PENDING for retry")
    void testPublisherConfirmationFailureOrTimeout() {
        OutboxEvent outboxEvent = new OutboxEvent(
            UUID.randomUUID().toString(),
            "payment.succeeded",
            "{\"webhookEventId\":\"" + UUID.randomUUID() + "\",\"eventId\":\"evt_nack_1\",\"source\":\"stripe\",\"eventType\":\"payment.succeeded\"}",
            OutboxStatus.PENDING
        );
        outboxEvent = outboxEventRepository.saveAndFlush(outboxEvent);

        doThrow(new WebhookPublishingException("Broker rejected message with NACK: timeout waiting for confirm"))
            .when(webhookEventPublisher).publishProcessingMessage(any());

        outboxPublisher.publishSingleEvent(outboxEvent);

        OutboxEvent afterFailure = outboxEventRepository.findById(outboxEvent.getId()).orElseThrow();
        assertThat(afterFailure.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(afterFailure.getAttemptCount()).isEqualTo(1);
        assertThat(afterFailure.getLastError()).contains("Broker rejected message with NACK");
        assertThat(afterFailure.getPublishedAt()).isNull();
    }

    // =========================================================================
    // 3. Worker failure during processing
    // =========================================================================
    @Test
    @DisplayName("Scenario 3: Worker failure during processing records failed attempt and sets RETRY_PENDING")
    void testWorkerFailureDuringProcessing() {
        WebhookEvent event = new WebhookEvent(
            "evt_worker_fail_1",
            "stripe",
            "invoice.payment_failed",
            "{\"amount\":100}",
            WebhookStatus.RECEIVED
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        doThrow(new RuntimeException("Simulated worker NullPointerException during event processing"))
            .when(webhookEventProcessor).processEvent(any());

        assertThatThrownBy(() -> webhookProcessingService.processEventById(savedEvent.getId()))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Simulated worker NullPointerException");

        WebhookEvent afterFailure = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(afterFailure.getStatus()).isEqualTo(WebhookStatus.RETRY_PENDING);
        assertThat(afterFailure.getAttemptCount()).isEqualTo(1);
        assertThat(afterFailure.getLastError()).contains("Simulated worker NullPointerException");
        assertThat(afterFailure.getNextRetryAt()).isNotNull();

        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(afterFailure);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(attempts.get(0).getErrorMessage()).contains("Simulated worker NullPointerException");
    }

    // =========================================================================
    // 4. Application restart while an outbox record is pending
    // =========================================================================
    @Test
    @DisplayName("Scenario 4: Application restart while outbox record is pending preserves record and publishes on recovery")
    void testApplicationRestartWhileOutboxRecordPending() {
        OutboxEvent pendingRecord = new OutboxEvent(
            UUID.randomUUID().toString(),
            "charge.refunded",
            "{\"webhookEventId\":\"" + UUID.randomUUID() + "\",\"eventId\":\"evt_restart_1\",\"source\":\"stripe\",\"eventType\":\"charge.refunded\"}",
            OutboxStatus.PENDING
        );
        pendingRecord = outboxEventRepository.saveAndFlush(pendingRecord);

        // Simulate application restart: outbox publisher runs on fresh cycle
        doNothing().when(webhookEventPublisher).publishProcessingMessage(any());

        outboxPublisher.processOutboxEvents();

        OutboxEvent afterRecovery = outboxEventRepository.findById(pendingRecord.getId()).orElseThrow();
        assertThat(afterRecovery.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(afterRecovery.getPublishedAt()).isNotNull();
    }

    // =========================================================================
    // 5. Retry scheduling and restart behavior
    // =========================================================================
    @Test
    @DisplayName("Scenario 5: Retry scheduling and restart behavior properly handles due and non-due events")
    void testRetrySchedulingAndRestartBehavior() {
        // Event not yet due
        WebhookEvent futureRetryEvent = new WebhookEvent(
            "evt_future_retry",
            "stripe",
            "charge.failed",
            "{}",
            WebhookStatus.RETRY_PENDING
        );
        futureRetryEvent.setNextRetryAt(Instant.now().plusSeconds(60));
        webhookEventRepository.saveAndFlush(futureRetryEvent);

        // Event that IS due
        WebhookEvent dueRetryEvent = new WebhookEvent(
            "evt_due_retry",
            "stripe",
            "charge.failed",
            "{}",
            WebhookStatus.RETRY_PENDING
        );
        dueRetryEvent.setNextRetryAt(Instant.now().minusSeconds(5));
        dueRetryEvent = webhookEventRepository.saveAndFlush(dueRetryEvent);

        // Run scheduler
        webhookRetryScheduler.scheduleDueRetries();

        // Verify future retry event was not modified
        WebhookEvent afterSchedulerFuture = webhookEventRepository.findById(futureRetryEvent.getId()).orElseThrow();
        assertThat(afterSchedulerFuture.getStatus()).isEqualTo(WebhookStatus.RETRY_PENDING);

        // Verify due retry event transitioned to RECEIVED and created OutboxEvent
        WebhookEvent afterSchedulerDue = webhookEventRepository.findById(dueRetryEvent.getId()).orElseThrow();
        assertThat(afterSchedulerDue.getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        List<OutboxEvent> outboxList = outboxEventRepository.findAll();
        assertThat(outboxList).hasSize(1);
        assertThat(outboxList.get(0).getAggregateId()).isEqualTo(dueRetryEvent.getId().toString());
        assertThat(outboxList.get(0).getStatus()).isEqualTo(OutboxStatus.PENDING);
    }

    // =========================================================================
    // 6. Repeated delivery of the same message
    // =========================================================================
    @Test
    @DisplayName("Scenario 6: Repeated delivery of already COMPLETED message is ignored idempotently")
    void testRepeatedDeliveryOfSameMessage() {
        WebhookEvent completedEvent = new WebhookEvent(
            "evt_completed_repeated",
            "stripe",
            "payment.succeeded",
            "{}",
            WebhookStatus.COMPLETED
        );
        completedEvent.setAttemptCount(1);
        completedEvent.setProcessedAt(Instant.now().minusSeconds(10));
        completedEvent = webhookEventRepository.saveAndFlush(completedEvent);

        // Deliver message again
        webhookProcessingService.processEventById(completedEvent.getId());

        // Verify no business logic ran, no duplicate attempt was created
        verify(webhookEventProcessor, never()).processEvent(any());
        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(completedEvent);
        assertThat(attempts).isEmpty();
        assertThat(webhookEventRepository.findById(completedEvent.getId()).orElseThrow().getStatus())
            .isEqualTo(WebhookStatus.COMPLETED);
    }

    // =========================================================================
    // 7. Concurrent workers attempting to process the same event
    // =========================================================================
    @Test
    @DisplayName("Scenario 7: Concurrent workers attempting to process the same event execute business logic exactly once")
    void testConcurrentWorkersProcessingSameEvent() throws Exception {
        WebhookEvent event = new WebhookEvent(
            "evt_concurrent_workers",
            "shopify",
            "orders.create",
            "{\"order\":42}",
            WebhookStatus.RECEIVED
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);

        doNothing().when(webhookEventProcessor).processEvent(any());

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    webhookProcessingService.processEventById(savedEvent.getId());
                    successCount.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // Exactly one processing run should have executed processor logic and attempt
        WebhookEvent afterConcurrent = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(afterConcurrent.getStatus()).isEqualTo(WebhookStatus.COMPLETED);
        assertThat(afterConcurrent.getAttemptCount()).isEqualTo(1);

        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(afterConcurrent);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatus()).isEqualTo(WebhookStatus.COMPLETED);

        verify(webhookEventProcessor, times(1)).processEvent(any());
    }

    // =========================================================================
    // 8. Concurrent outbox publishers attempting to claim the same record
    // =========================================================================
    @Test
    @DisplayName("Scenario 8: Concurrent outbox publishers attempting to claim the same record safely publish without errors")
    void testConcurrentOutboxPublishers() throws Exception {
        OutboxEvent outboxEvent = new OutboxEvent(
            UUID.randomUUID().toString(),
            "payment.succeeded",
            "{\"webhookEventId\":\"" + UUID.randomUUID() + "\",\"eventId\":\"evt_concurrent_pub\",\"source\":\"stripe\",\"eventType\":\"payment.succeeded\"}",
            OutboxStatus.PENDING
        );
        OutboxEvent savedOutbox = outboxEventRepository.saveAndFlush(outboxEvent);

        doNothing().when(webhookEventPublisher).publishProcessingMessage(any());

        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    outboxPublisher.publishSingleEvent(savedOutbox);
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        OutboxEvent afterConcurrent = outboxEventRepository.findById(savedOutbox.getId()).orElseThrow();
        assertThat(afterConcurrent.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(afterConcurrent.getPublishedAt()).isNotNull();
    }

    // =========================================================================
    // 9. Duplicate replay requests
    // =========================================================================
    @Test
    @DisplayName("Scenario 9: Duplicate replay requests result in exactly one successful replay and one 409 Conflict")
    void testDuplicateReplayRequests() throws Exception {
        WebhookEvent deadLetterEvent = new WebhookEvent(
            "evt_dlq_duplicate_replay",
            "stripe",
            "charge.failed",
            "{}",
            WebhookStatus.DEAD_LETTER
        );
        deadLetterEvent.setAttemptCount(3);
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(deadLetterEvent);

        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    deadLetterService.replayDeadLetter(savedEvent.getId(), "Concurrent replay attempt");
                    successCount.incrementAndGet();
                } catch (InvalidStateTransitionException e) {
                    conflictCount.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(conflictCount.get()).isEqualTo(1);

        // Verify DB state: exactly 1 replay audit and 1 outbox record created
        WebhookEvent replayedEvent = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(replayedEvent.getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        List<WebhookReplay> replays = webhookReplayRepository.findByWebhookEventOrderByRequestedAtDesc(replayedEvent);
        assertThat(replays).hasSize(1);
        assertThat(outboxEventRepository.findAll()).hasSize(1);
    }

    // =========================================================================
    // 10. Unauthorized replay attempts
    // =========================================================================
    @Test
    @DisplayName("Scenario 10: Unauthorized replay attempts rejected with 401 Unauthorized or 403 Forbidden")
    void testUnauthorizedReplayAttempts() throws Exception {
        WebhookEvent deadLetterEvent = new WebhookEvent(
            "evt_dlq_unauth",
            "stripe",
            "charge.failed",
            "{}",
            WebhookStatus.DEAD_LETTER
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(deadLetterEvent);

        String replayPayload = "{\"reason\":\"Unauthorized attempt\"}";

        // 1. Unauthenticated -> 401
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", savedEvent.getId())
                .contentType(MediaType.APPLICATION_JSON)
                .content(replayPayload))
            .andExpect(status().isUnauthorized());

        // 2. Authenticated as VIEWER (requires ADMIN or OPERATOR) -> 403
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", savedEvent.getId())
                .header("Authorization", viewerAuthHeader)
                .contentType(MediaType.APPLICATION_JSON)
                .content(replayPayload))
            .andExpect(status().isForbidden());

        // Verify DB unchanged: status remains DEAD_LETTER, no replays created
        WebhookEvent unchangedEvent = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(unchangedEvent.getStatus()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(webhookReplayRepository.findAll()).isEmpty();
        assertThat(outboxEventRepository.findAll()).isEmpty();
    }

    // =========================================================================
    // 11. Redis unavailable during public ingestion
    // =========================================================================
    @Test
    @DisplayName("Scenario 11: Redis unavailable during public ingestion returns 503 Service Unavailable without database writes")
    void testRedisUnavailableDuringPublicIngestion() throws Exception {
        when(rateLimiterService.tryAcquire(any()))
            .thenThrow(new RateLimiterUnavailableException("Redis rate limiter connection timed out"));

        String ingestPayload = """
            {
                "eventId": "evt_redis_down",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 100}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(ingestPayload))
            .andExpect(status().isServiceUnavailable())
            .andExpect(header().string("Retry-After", "30"))
            .andExpect(jsonPath("$.status").value(503))
            .andExpect(jsonPath("$.error").value("Service Unavailable"));

        // Verify no database writes occurred
        assertThat(webhookEventRepository.findAll()).isEmpty();
        assertThat(outboxEventRepository.findAll()).isEmpty();
    }

    // =========================================================================
    // 12. Rate-limit exhaustion without database writes
    // =========================================================================
    @Test
    @DisplayName("Scenario 12: Rate-limit exhaustion returns 429 Too Many Requests without database writes")
    void testRateLimitExhaustionWithoutDatabaseWrites() throws Exception {
        when(rateLimiterService.tryAcquire(any()))
            .thenReturn(RateLimitResult.denied(60, 45));

        String ingestPayload = """
            {
                "eventId": "evt_rate_limited",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 100}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(ingestPayload))
            .andExpect(status().isTooManyRequests())
            .andExpect(header().string("Retry-After", "45"))
            .andExpect(header().string("X-RateLimit-Limit", "60"))
            .andExpect(header().string("X-RateLimit-Remaining", "0"))
            .andExpect(jsonPath("$.status").value(429))
            .andExpect(jsonPath("$.error").value("Too Many Requests"));

        // Verify no database writes occurred
        assertThat(webhookEventRepository.findAll()).isEmpty();
        assertThat(outboxEventRepository.findAll()).isEmpty();
    }

    // =========================================================================
    // 13. External reconciliation provider failure
    // =========================================================================
    @Test
    @DisplayName("Scenario 13: External reconciliation provider failure records FAILED result and does not abort batch")
    void testExternalReconciliationProviderFailure() {
        WebhookEvent event = new WebhookEvent(
            "evt_prov_failure_1",
            "stripe",
            "payment.succeeded",
            "{}",
            WebhookStatus.COMPLETED
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        simulatedSourceOfTruthClient.registerFailure("stripe", "evt_prov_failure_1", "Simulated 500 External Upstream Gateway Error");

        ReconciliationRecord record = reconciliationService.reconcileSingleEvent(savedEvent, false);

        assertThat(record).isNotNull();
        assertThat(record.getResult()).isEqualTo(ReconciliationResult.FAILED);
        assertThat(record.getExternalStatus()).isNull();
        assertThat(record.getDetails()).contains("PROVIDER_ERROR");
        assertThat(record.getDetails()).contains("Simulated 500 External Upstream Gateway Error");

        ReconciliationRecord dbRecord = reconciliationRecordRepository.findById(record.getId()).orElseThrow();
        assertThat(dbRecord.getResult()).isEqualTo(ReconciliationResult.FAILED);
    }

    // =========================================================================
    // 14. Concurrent reconciliation of the same event
    // =========================================================================
    @Test
    @DisplayName("Scenario 14: Concurrent reconciliation of the same event safe against unique constraint races")
    void testConcurrentReconciliationOfSameEvent() throws Exception {
        WebhookEvent event = new WebhookEvent(
            "evt_recon_concurrent",
            "stripe",
            "payment.succeeded",
            "{}",
            WebhookStatus.COMPLETED
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ReconciliationRecord r = reconciliationService.reconcileSingleEvent(savedEvent, false);
                    if (r != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(2);

        // Exactly 1 reconciliation record in the database for this event
        List<ReconciliationRecord> records = reconciliationRecordRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getWebhookEvent().getId()).isEqualTo(savedEvent.getId());
        assertThat(records.get(0).getResult()).isEqualTo(ReconciliationResult.MATCHED);
    }
}
