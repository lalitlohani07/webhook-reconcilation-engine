package com.example.webhookengine.integration;

import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.controller.dto.ReconciliationRunResponse;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookReplay;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.messaging.OutboxPublisher;
import com.example.webhookengine.messaging.WebhookEventPublisher;
import com.example.webhookengine.messaging.WebhookProcessingMessage;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import com.example.webhookengine.ratelimit.service.RateLimiterService;
import com.example.webhookengine.reconciliation.provider.ExternalEventRecord;
import com.example.webhookengine.reconciliation.provider.SimulatedSourceOfTruthClient;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.ReconciliationRecordRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.repository.WebhookReplayRepository;
import com.example.webhookengine.security.domain.SecurityRole;
import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import com.example.webhookengine.service.WebhookEventProcessor;
import com.example.webhookengine.service.WebhookProcessingService;
import com.example.webhookengine.service.WebhookRetryScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
import org.springframework.test.web.servlet.MvcResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestFlywayConfig.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:e2e_lifecycle_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
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
public class EndToEndLifecycleIntegrationTest {

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

    private String operatorAuthHeader;

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

        SecurityUser operatorUser = new SecurityUser(
            UUID.randomUUID(),
            "operator_user",
            passwordEncoder.encode("operator_pass"),
            SecurityRole.OPERATOR,
            true
        );
        securityUserRepository.saveAndFlush(operatorUser);

        operatorAuthHeader = "Basic " + Base64.getEncoder().encodeToString("operator_user:operator_pass".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Complete Phase 9 End-to-End Workflow: Ingest -> Outbox -> Worker -> Retry -> DLQ -> Inspect -> Replay -> Reconcile")
    void testCompleteEndToEndLifecycleWorkflow() throws Exception {
        // =========================================================================
        // STEP 1: Submit a webhook through POST /api/v1/webhooks
        // =========================================================================
        String ingestPayload = """
            {
                "eventId": "evt_e2e_full_lifecycle",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {
                    "id": "ch_12345",
                    "amount": 9900,
                    "currency": "usd"
                }
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(ingestPayload))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.status").value("RECEIVED"))
            .andExpect(jsonPath("$.message").value("Webhook accepted for processing"))
            .andExpect(jsonPath("$.eventId").value("evt_e2e_full_lifecycle"))
            .andExpect(jsonPath("$.source").value("stripe"));

        // =========================================================================
        // STEP 2: Verify the WebhookEvent and OutboxEvent are persisted
        // =========================================================================
        WebhookEvent ingestedEvent = webhookEventRepository.findBySourceAndEventId("stripe", "evt_e2e_full_lifecycle")
            .orElseThrow(() -> new AssertionError("WebhookEvent was not persisted"));

        assertThat(ingestedEvent.getStatus()).isEqualTo(WebhookStatus.RECEIVED);
        assertThat(ingestedEvent.getAttemptCount()).isEqualTo(0);
        assertThat(ingestedEvent.getPayload()).contains("ch_12345");

        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).hasSize(1);
        OutboxEvent initialOutbox = outboxEvents.get(0);
        assertThat(initialOutbox.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(initialOutbox.getAggregateId()).isEqualTo(ingestedEvent.getId().toString());
        assertThat(initialOutbox.getEventType()).isEqualTo("payment.succeeded");

        // =========================================================================
        // STEP 3: Verify the outbox publisher sends the event to RabbitMQ
        // =========================================================================
        ArgumentCaptor<WebhookProcessingMessage> messageCaptor = ArgumentCaptor.forClass(WebhookProcessingMessage.class);
        doNothing().when(webhookEventPublisher).publishProcessingMessage(messageCaptor.capture());

        outboxPublisher.processOutboxEvents();

        verify(webhookEventPublisher, times(1)).publishProcessingMessage(any());
        WebhookProcessingMessage publishedMessage = messageCaptor.getValue();
        assertThat(publishedMessage.webhookEventId()).isEqualTo(ingestedEvent.getId());
        assertThat(publishedMessage.eventId()).isEqualTo("evt_e2e_full_lifecycle");

        OutboxEvent updatedOutbox = outboxEventRepository.findById(initialOutbox.getId()).orElseThrow();
        assertThat(updatedOutbox.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(updatedOutbox.getPublishedAt()).isNotNull();

        // =========================================================================
        // STEP 4 & 5: Simulate a retryable failure and verify exponential backoff
        // =========================================================================
        // Attempt 1: Transient 503 error
        doThrow(new RuntimeException("Transient 503 Upstream Gateway Error"))
            .when(webhookEventProcessor).processEvent(any());

        assertThatThrownBy(() -> webhookProcessingService.processEventById(ingestedEvent.getId()))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Transient 503");

        WebhookEvent afterAttempt1 = webhookEventRepository.findById(ingestedEvent.getId()).orElseThrow();
        assertThat(afterAttempt1.getStatus()).isEqualTo(WebhookStatus.RETRY_PENDING);
        assertThat(afterAttempt1.getAttemptCount()).isEqualTo(1);
        assertThat(afterAttempt1.getLastError()).contains("Transient 503");
        assertThat(afterAttempt1.getNextRetryAt()).isNotNull();

        List<WebhookAttempt> attemptsAfter1 = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(afterAttempt1);
        assertThat(attemptsAfter1).hasSize(1);
        assertThat(attemptsAfter1.get(0).getAttemptNumber()).isEqualTo(1);
        assertThat(attemptsAfter1.get(0).getStatus()).isEqualTo(WebhookStatus.FAILED);

        // Attempt 2: Re-enqueue via scheduler and fail again
        afterAttempt1.setNextRetryAt(Instant.now().minusSeconds(1));
        webhookEventRepository.saveAndFlush(afterAttempt1);

        webhookRetryScheduler.scheduleDueRetries();
        outboxPublisher.processOutboxEvents();
        WebhookEvent afterScheduler2 = webhookEventRepository.findById(ingestedEvent.getId()).orElseThrow();
        assertThat(afterScheduler2.getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        assertThatThrownBy(() -> webhookProcessingService.processEventById(ingestedEvent.getId()))
            .isInstanceOf(RuntimeException.class);

        WebhookEvent afterAttempt2 = webhookEventRepository.findById(ingestedEvent.getId()).orElseThrow();
        assertThat(afterAttempt2.getStatus()).isEqualTo(WebhookStatus.RETRY_PENDING);
        assertThat(afterAttempt2.getAttemptCount()).isEqualTo(2);

        // =========================================================================
        // STEP 6: Verify exhausted events enter DEAD_LETTER
        // =========================================================================
        // Attempt 3 of 3 (maxAttempts = 3)
        afterAttempt2.setNextRetryAt(Instant.now().minusSeconds(1));
        webhookEventRepository.saveAndFlush(afterAttempt2);

        webhookRetryScheduler.scheduleDueRetries();
        outboxPublisher.processOutboxEvents();
        assertThatThrownBy(() -> webhookProcessingService.processEventById(ingestedEvent.getId()))
            .isInstanceOf(RuntimeException.class);

        WebhookEvent exhaustedEvent = webhookEventRepository.findById(ingestedEvent.getId()).orElseThrow();
        assertThat(exhaustedEvent.getStatus()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(exhaustedEvent.getAttemptCount()).isEqualTo(3);
        assertThat(exhaustedEvent.getNextRetryAt()).isNull();

        List<WebhookAttempt> allAttempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(exhaustedEvent);
        assertThat(allAttempts).hasSize(3);

        // =========================================================================
        // STEP 7: Inspect the dead letter using the protected API
        // =========================================================================
        mockMvc.perform(get("/api/v1/dead-letters/{id}", exhaustedEvent.getId())
                .header("Authorization", operatorAuthHeader))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(exhaustedEvent.getId().toString()))
            .andExpect(jsonPath("$.status").value("DEAD_LETTER"))
            .andExpect(jsonPath("$.attemptCount").value(3))
            .andExpect(jsonPath("$.attempts.length()").value(3))
            .andExpect(jsonPath("$.replays.length()").value(0));

        // =========================================================================
        // STEP 8: Replay it with an authorized account
        // =========================================================================
        String replayPayload = """
            {
                "reason": "Operator inspected failure and resolved upstream dependency"
            }
            """;

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", exhaustedEvent.getId())
                .header("Authorization", operatorAuthHeader)
                .contentType(MediaType.APPLICATION_JSON)
                .content(replayPayload))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.id").value(exhaustedEvent.getId().toString()))
            .andExpect(jsonPath("$.status").value("RECEIVED"))
            .andExpect(jsonPath("$.message").value("Webhook event replay accepted and enqueued for processing"));

        // =========================================================================
        // STEP 9: Verify replay audit history and outbox publication
        // =========================================================================
        WebhookEvent replayedEvent = webhookEventRepository.findById(exhaustedEvent.getId()).orElseThrow();
        assertThat(replayedEvent.getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        List<WebhookReplay> replays = webhookReplayRepository.findByWebhookEventOrderByRequestedAtDesc(replayedEvent);
        assertThat(replays).hasSize(1);
        assertThat(replays.get(0).getReason()).isEqualTo("Operator inspected failure and resolved upstream dependency");
        assertThat(replays.get(0).getPreviousAttemptCount()).isEqualTo(3);

        // Verify outbox record created for replay
        List<OutboxEvent> replayOutboxList = outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);
        assertThat(replayOutboxList).hasSize(1);
        assertThat(replayOutboxList.get(0).getAggregateId()).isEqualTo(replayedEvent.getId().toString());

        // Publish replay outbox
        outboxPublisher.processOutboxEvents();
        assertThat(outboxEventRepository.findById(replayOutboxList.get(0).getId()).orElseThrow().getStatus())
            .isEqualTo(OutboxStatus.PUBLISHED);

        // Worker processes the replayed event (now upstream is fixed)
        doNothing().when(webhookEventProcessor).processEvent(any());
        webhookProcessingService.processEventById(replayedEvent.getId());

        WebhookEvent completedEvent = webhookEventRepository.findById(replayedEvent.getId()).orElseThrow();
        assertThat(completedEvent.getStatus()).isEqualTo(WebhookStatus.COMPLETED);
        assertThat(completedEvent.getProcessedAt()).isNotNull();

        List<WebhookAttempt> finalAttempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(completedEvent);
        assertThat(finalAttempts).hasSize(4);
        assertThat(finalAttempts.get(3).getAttemptNumber()).isEqualTo(4);
        assertThat(finalAttempts.get(3).getStatus()).isEqualTo(WebhookStatus.COMPLETED);

        // =========================================================================
        // STEP 10: Run reconciliation and verify the expected result is recorded
        // =========================================================================
        simulatedSourceOfTruthClient.registerRecord("stripe", "evt_e2e_full_lifecycle", new ExternalEventRecord(
            "evt_e2e_full_lifecycle",
            "stripe",
            "COMPLETED",
            "payment.succeeded",
            Instant.now(),
            Map.of("reconciledBy", "test")
        ));

        String reconPayload = """
            {
                "status": "COMPLETED",
                "source": "stripe",
                "batchSize": 10,
                "includeAlreadyReconciled": false
            }
            """;

        MvcResult reconResult = mockMvc.perform(post("/api/v1/reconciliation/runs")
                .header("Authorization", operatorAuthHeader)
                .contentType(MediaType.APPLICATION_JSON)
                .content(reconPayload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.totalEvaluated").value(1))
            .andExpect(jsonPath("$.matchedCount").value(1))
            .andExpect(jsonPath("$.mismatchCount").value(0))
            .andExpect(jsonPath("$.failedCount").value(0))
            .andReturn();

        ReconciliationRunResponse reconResponse = objectMapper.readValue(
            reconResult.getResponse().getContentAsString(),
            ReconciliationRunResponse.class
        );

        UUID recordId = reconResponse.records().get(0).id();

        // Inspect through GET /api/v1/reconciliation/records/{id}
        mockMvc.perform(get("/api/v1/reconciliation/records/{id}", recordId)
                .header("Authorization", operatorAuthHeader))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(recordId.toString()))
            .andExpect(jsonPath("$.result").value("MATCHED"))
            .andExpect(jsonPath("$.internalStatus").value("COMPLETED"))
            .andExpect(jsonPath("$.externalStatus").value("COMPLETED"));

        ReconciliationRecord dbRecord = reconciliationRecordRepository.findById(recordId).orElseThrow();
        assertThat(dbRecord.getResult()).isEqualTo(ReconciliationResult.MATCHED);
    }
}
