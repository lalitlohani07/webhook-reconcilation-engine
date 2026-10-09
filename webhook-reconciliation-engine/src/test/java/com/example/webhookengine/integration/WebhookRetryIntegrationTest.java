package com.example.webhookengine.integration;

import com.example.webhookengine.config.RetryProperties;
import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.service.RetryPolicy;
import com.example.webhookengine.service.WebhookEventProcessor;
import com.example.webhookengine.service.WebhookProcessingService;
import com.example.webhookengine.service.WebhookRetryScheduler;
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

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@DataJpaTest
@Import({
    TestFlywayConfig.class,
    RetryProperties.class,
    RetryPolicy.class,
    WebhookProcessingService.class,
    WebhookRetryScheduler.class,
    ObjectMapper.class
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:webhook_retry_integration_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "app.retry.enabled=true",
    "app.retry.max-attempts=3",
    "app.retry.initial-delay-ms=1000",
    "app.retry.multiplier=2.0",
    "app.retry.max-delay-ms=60000"
})
class WebhookRetryIntegrationTest {

    @Autowired
    private WebhookProcessingService webhookProcessingService;

    @Autowired
    private WebhookRetryScheduler webhookRetryScheduler;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private WebhookAttemptRepository webhookAttemptRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockitoBean
    private WebhookEventProcessor webhookEventProcessor;

    @BeforeEach
    void setUp() {
        webhookAttemptRepository.deleteAll();
        outboxEventRepository.deleteAll();
        webhookEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Transient failure workflow: creates failed attempt, sets RETRY_PENDING with nextRetryAt, and scheduler re-enqueues via outbox")
    void shouldExecuteTransientFailureWorkflowAndScheduleRetry() {
        WebhookEvent event = new WebhookEvent("evt_transient", "stripe", "charge.failed", "{\"amount\":100}", WebhookStatus.RECEIVED);
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        // Processor throws transient failure
        doThrow(new RuntimeException("Simulated 503 Service Unavailable"))
            .when(webhookEventProcessor).processEvent(any());

        assertThatThrownBy(() -> webhookProcessingService.processEventById(savedEvent.getId()))
            .isInstanceOf(RuntimeException.class);

        // Verify database state after first execution
        WebhookEvent afterFailure = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(afterFailure.getStatus()).isEqualTo(WebhookStatus.RETRY_PENDING);
        assertThat(afterFailure.getAttemptCount()).isEqualTo(1);
        assertThat(afterFailure.getLastError()).isEqualTo("Simulated 503 Service Unavailable");
        assertThat(afterFailure.getNextRetryAt()).isNotNull();

        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(afterFailure);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getAttemptNumber()).isEqualTo(1);
        assertThat(attempts.get(0).getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(attempts.get(0).getErrorMessage()).isEqualTo("Simulated 503 Service Unavailable");

        // Fast-forward nextRetryAt into the past so it is due
        afterFailure.setNextRetryAt(Instant.now().minusSeconds(5));
        webhookEventRepository.saveAndFlush(afterFailure);

        // Run scheduler
        webhookRetryScheduler.scheduleDueRetries();

        // Verify event was claimed (status -> RECEIVED) and OutboxEvent was created in PENDING
        WebhookEvent afterSchedule = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(afterSchedule.getStatus()).isEqualTo(WebhookStatus.RECEIVED);

        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).hasSize(1);
        assertThat(outboxEvents.get(0).getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvents.get(0).getAggregateId()).isEqualTo(savedEvent.getId().toString());
        assertThat(outboxEvents.get(0).getPayload()).contains("evt_transient");
    }

    @Test
    @DisplayName("Exhausted retries workflow: sets FAILED status and scheduler does not pick it up")
    void shouldExhaustRetriesAndSetFailedStatus() {
        WebhookEvent event = new WebhookEvent("evt_exhausted", "stripe", "charge.failed", "{}", WebhookStatus.RECEIVED);
        event.setAttemptCount(2); // Initial attempt count 2; next will be 3 of 3 (maxAttempts = 3)
        WebhookEvent savedEvent = webhookEventRepository.saveAndFlush(event);

        doThrow(new RuntimeException("Permanent error"))
            .when(webhookEventProcessor).processEvent(any());

        assertThatThrownBy(() -> webhookProcessingService.processEventById(savedEvent.getId()))
            .isInstanceOf(RuntimeException.class);

        WebhookEvent afterFailure = webhookEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(afterFailure.getStatus()).isEqualTo(WebhookStatus.FAILED);
        assertThat(afterFailure.getAttemptCount()).isEqualTo(3);
        assertThat(afterFailure.getNextRetryAt()).isNull();

        // Run scheduler and verify no outbox event is created
        webhookRetryScheduler.scheduleDueRetries();
        assertThat(outboxEventRepository.findAll()).isEmpty();
    }
}
