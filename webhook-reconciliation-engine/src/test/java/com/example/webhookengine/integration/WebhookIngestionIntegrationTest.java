package com.example.webhookengine.integration;

import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.controller.dto.WebhookIngestRequest;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.service.IngestionResult;
import com.example.webhookengine.service.WebhookIngestionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Import({TestFlywayConfig.class, WebhookIngestionService.class, ObjectMapper.class})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:webhook_ingestion_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver"
})
class WebhookIngestionIntegrationTest {

    @Autowired
    private WebhookIngestionService webhookIngestionService;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private com.example.webhookengine.repository.OutboxEventRepository outboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
        webhookEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Valid webhook ingestion saves event with status RECEIVED")
    void shouldIngestAndPersistWebhookEvent() {
        WebhookIngestRequest request = new WebhookIngestRequest(
            "evt_001",
            "stripe",
            "payment_intent.succeeded",
            objectMapper.createObjectNode().put("amount", 5000).put("currency", "INR")
        );

        IngestionResult result = webhookIngestionService.ingestWebhook(request);

        assertThat(result.isNew()).isTrue();
        assertThat(result.event().getId()).isNotNull();
        assertThat(result.event().getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        Optional<WebhookEvent> persisted = webhookEventRepository.findBySourceAndEventId("stripe", "evt_001");
        assertThat(persisted).isPresent();
        assertThat(persisted.get().getPayload()).contains("5000");

        List<com.example.webhookengine.domain.OutboxEvent> outboxList = outboxEventRepository.findAll();
        assertThat(outboxList).hasSize(1);
        assertThat(outboxList.get(0).getStatus()).isEqualTo(com.example.webhookengine.domain.OutboxStatus.PENDING);
        assertThat(outboxList.get(0).getAggregateId()).isEqualTo(result.event().getId().toString());
    }

    @Test
    @DisplayName("Duplicate request does not create duplicate record or overwrite original payload")
    void shouldNotDuplicateOrOverwriteOnDuplicateRequest() {
        WebhookIngestRequest initialRequest = new WebhookIngestRequest(
            "evt_002",
            "stripe",
            "payment_intent.succeeded",
            objectMapper.createObjectNode().put("initial", "true")
        );

        IngestionResult firstResult = webhookIngestionService.ingestWebhook(initialRequest);
        assertThat(firstResult.isNew()).isTrue();

        // Mutate status to PROCESSING to simulate worker pickup
        WebhookEvent initialEvent = firstResult.event();
        initialEvent.setStatus(WebhookStatus.PROCESSING);
        webhookEventRepository.saveAndFlush(initialEvent);

        // Send duplicate request with different payload attempt
        WebhookIngestRequest duplicateRequest = new WebhookIngestRequest(
            "evt_002",
            "stripe",
            "payment_intent.succeeded",
            objectMapper.createObjectNode().put("new_attempt", "false")
        );

        IngestionResult secondResult = webhookIngestionService.ingestWebhook(duplicateRequest);

        assertThat(secondResult.isNew()).isFalse();
        assertThat(secondResult.event().getStatus()).isEqualTo(WebhookStatus.PROCESSING);
        assertThat(secondResult.event().getPayload()).contains("initial");
        assertThat(secondResult.event().getPayload()).doesNotContain("new_attempt");
        assertThat(webhookEventRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Different sources with same event ID creates separate records")
    void shouldAllowSameEventIdFromDifferentSources() {
        WebhookIngestRequest stripeRequest = new WebhookIngestRequest(
            "evt_common",
            "stripe",
            "payment.succeeded",
            objectMapper.createObjectNode().put("source", "stripe")
        );

        WebhookIngestRequest githubRequest = new WebhookIngestRequest(
            "evt_common",
            "github",
            "workflow_job.completed",
            objectMapper.createObjectNode().put("source", "github")
        );

        IngestionResult stripeResult = webhookIngestionService.ingestWebhook(stripeRequest);
        IngestionResult githubResult = webhookIngestionService.ingestWebhook(githubRequest);

        assertThat(stripeResult.isNew()).isTrue();
        assertThat(githubResult.isNew()).isTrue();
        assertThat(webhookEventRepository.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("Same source with different event IDs creates separate records")
    void shouldAllowDifferentEventIdsFromSameSource() {
        WebhookIngestRequest req1 = new WebhookIngestRequest("evt_a", "stripe", "order.created", objectMapper.createObjectNode());
        WebhookIngestRequest req2 = new WebhookIngestRequest("evt_b", "stripe", "order.created", objectMapper.createObjectNode());

        IngestionResult res1 = webhookIngestionService.ingestWebhook(req1);
        IngestionResult res2 = webhookIngestionService.ingestWebhook(req2);

        assertThat(res1.isNew()).isTrue();
        assertThat(res2.isNew()).isTrue();
        assertThat(webhookEventRepository.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("Concurrent duplicate requests result in exactly one database record")
    void shouldHandleConcurrentDuplicateRequests() throws InterruptedException {
        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger newCount = new AtomicInteger(0);
        AtomicInteger existingCount = new AtomicInteger(0);

        WebhookIngestRequest request = new WebhookIngestRequest(
            "evt_concurrent",
            "stripe",
            "charge.succeeded",
            objectMapper.createObjectNode().put("test", "concurrency")
        );

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    IngestionResult result = webhookIngestionService.ingestWebhook(request);
                    if (result.isNew()) {
                        newCount.incrementAndGet();
                    } else {
                        existingCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // unexpected
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        doneLatch.await(5, TimeUnit.SECONDS);
        assertThat(webhookEventRepository.count()).isEqualTo(1);
        assertThat(outboxEventRepository.count()).isEqualTo(1);
        assertThat(newCount.get()).isEqualTo(1);
        assertThat(existingCount.get()).isEqualTo(threadCount - 1);
    }
}
