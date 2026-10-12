package com.example.webhookengine.integration;

import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.messaging.OutboxPublisher;
import com.example.webhookengine.messaging.WebhookEventPublisher;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import com.example.webhookengine.ratelimit.service.RateLimiterService;
import com.example.webhookengine.reconciliation.provider.SimulatedSourceOfTruthClient;
import com.example.webhookengine.reconciliation.service.ReconciliationServiceImpl;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.ReconciliationRecordRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.service.WebhookEventProcessor;
import com.example.webhookengine.service.WebhookProcessingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Real PostgreSQL and RabbitMQ integration tests using Testcontainers.
 * Executes true PostgreSQL locking, isolation, and transaction semantics when Docker is available.
 * Skips gracefully when Docker daemon access is restricted.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@EnabledIf("isDockerAvailable")
public class PostgresReliabilityTestcontainersIntegrationTest {

    static boolean isDockerAvailable() {
        try {
            return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("webhook_engine_pg_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    @Container
    static RabbitMQContainer rabbitmq = new RabbitMQContainer("rabbitmq:3-management-alpine");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.rabbitmq.host", rabbitmq::getHost);
        registry.add("spring.rabbitmq.port", rabbitmq::getAmqpPort);
        registry.add("spring.rabbitmq.username", () -> "guest");
        registry.add("spring.rabbitmq.password", () -> "guest");
        registry.add("management.health.redis.enabled", () -> "false");
        registry.add("management.health.rabbit.enabled", () -> "false");
        registry.add("spring.data.redis.host", () -> "localhost");
        registry.add("spring.data.redis.port", () -> "16379");
        registry.add("app.outbox.enabled", () -> "false");
        registry.add("app.retry.enabled", () -> "false");
    }

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private WebhookAttemptRepository webhookAttemptRepository;

    @Autowired
    private ReconciliationRecordRepository reconciliationRecordRepository;

    @Autowired
    private WebhookProcessingService webhookProcessingService;

    @Autowired
    private ReconciliationServiceImpl reconciliationService;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @MockitoSpyBean
    private WebhookEventProcessor webhookEventProcessor;

    @MockitoBean
    private RateLimiterService rateLimiterService;

    @BeforeEach
    void setUp() {
        reconciliationRecordRepository.deleteAll();
        webhookAttemptRepository.deleteAll();
        outboxEventRepository.deleteAll();
        webhookEventRepository.deleteAll();

        when(rateLimiterService.tryAcquire(any())).thenReturn(RateLimitResult.allowed(60, 59));
    }

    @Test
    @DisplayName("Real PostgreSQL: Concurrent workers competing on same event execute exactly once via SELECT FOR UPDATE")
    void testRealPostgreSqlConcurrentWorkerExecution() throws Exception {
        WebhookEvent event = new WebhookEvent(
            "evt_pg_concurrent_worker",
            "stripe",
            "payment.captured",
            "{\"amount\":5000}",
            WebhookStatus.RECEIVED
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        doNothing().when(webhookEventProcessor).processEvent(any());

        int threads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    webhookProcessingService.processEventById(savedEvent.getId());
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        WebhookEvent finalizedEvent = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(finalizedEvent.getStatus()).isEqualTo(WebhookStatus.COMPLETED);
        assertThat(finalizedEvent.getAttemptCount()).isEqualTo(1);

        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(finalizedEvent);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatus()).isEqualTo(WebhookStatus.COMPLETED);

        verify(webhookEventProcessor, times(1)).processEvent(any());
    }

    @Test
    @DisplayName("Real PostgreSQL: Concurrent reconciliation safely locks event without transaction aborts")
    void testRealPostgreSqlConcurrentReconciliation() throws Exception {
        WebhookEvent event = new WebhookEvent(
            "evt_pg_concurrent_recon",
            "stripe",
            "payment.captured",
            "{\"amount\":5000}",
            WebhookStatus.COMPLETED
        );
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        int threads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ReconciliationRecord record = reconciliationService.reconcileSingleEvent(savedEvent, false);
                    if (record != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(threads);

        List<ReconciliationRecord> records = reconciliationRecordRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getResult()).isEqualTo(ReconciliationResult.MATCHED);
    }
}
