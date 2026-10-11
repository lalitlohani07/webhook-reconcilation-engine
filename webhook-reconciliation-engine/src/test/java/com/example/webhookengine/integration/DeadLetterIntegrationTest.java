package com.example.webhookengine.integration;

import com.example.webhookengine.config.RetryProperties;
import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.controller.dto.DeadLetterDetailResponse;
import com.example.webhookengine.controller.dto.DeadLetterSummaryResponse;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.domain.OutboxEvent;
import com.example.webhookengine.domain.OutboxStatus;
import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookReplay;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.InvalidStateTransitionException;
import com.example.webhookengine.exception.ResourceNotFoundException;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookAttemptRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.repository.WebhookReplayRepository;
import com.example.webhookengine.service.DeadLetterService;
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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

@DataJpaTest
@Import({
    TestFlywayConfig.class,
    RetryProperties.class,
    RetryPolicy.class,
    WebhookProcessingService.class,
    WebhookRetryScheduler.class,
    DeadLetterService.class,
    ObjectMapper.class
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:dead_letter_integration_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "app.retry.enabled=true",
    "app.retry.max-attempts=3",
    "app.retry.initial-delay-ms=1000",
    "app.retry.multiplier=2.0",
    "app.retry.max-delay-ms=60000"
})
class DeadLetterIntegrationTest {

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private WebhookProcessingService webhookProcessingService;

    @Autowired
    private WebhookRetryScheduler webhookRetryScheduler;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private WebhookAttemptRepository webhookAttemptRepository;

    @Autowired
    private WebhookReplayRepository webhookReplayRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockitoBean
    private WebhookEventProcessor webhookEventProcessor;

    @BeforeEach
    void setUp() {
        webhookReplayRepository.deleteAll();
        webhookAttemptRepository.deleteAll();
        outboxEventRepository.deleteAll();
        webhookEventRepository.deleteAll();
    }

    @Test
    @DisplayName("End-to-end failure to DEAD_LETTER: exhausted attempts transition event to DEAD_LETTER and clear nextRetryAt")
    void shouldTransitionToDeadLetterWhenRetriesExhausted() {
        WebhookEvent event = new WebhookEvent("evt_dlq_1", "stripe", "charge.failed", "{\"amount\":500}", WebhookStatus.RECEIVED);
        event.setAttemptCount(2); // Initial attempt count 2; next execution will be attempt 3 of 3 (maxAttempts = 3)
        WebhookEvent saved = webhookEventRepository.saveAndFlush(event);

        doThrow(new RuntimeException("Simulated third fatal failure"))
            .when(webhookEventProcessor).processEvent(any());

        assertThatThrownBy(() -> webhookProcessingService.processEventById(saved.getId()))
            .isInstanceOf(RuntimeException.class);

        WebhookEvent afterFailure = webhookEventRepository.findById(saved.getId()).orElseThrow();
        assertThat(afterFailure.getStatus()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(afterFailure.getAttemptCount()).isEqualTo(3);
        assertThat(afterFailure.getNextRetryAt()).isNull();
        assertThat(afterFailure.getLastError()).isEqualTo("Simulated third fatal failure");

        // WebhookRetryScheduler must never pick up DEAD_LETTER events
        webhookRetryScheduler.scheduleDueRetries();
        assertThat(outboxEventRepository.findAll()).isEmpty();
    }

    @Test
    @DisplayName("List and inspect dead-letter endpoints query database accurately")
    void shouldListAndInspectDeadLetterEvents() {
        WebhookEvent deadEvent = new WebhookEvent("evt_dead_2", "shopify", "order.cancelled", "{}", WebhookStatus.DEAD_LETTER);
        deadEvent.setAttemptCount(3);
        deadEvent.setLastError("Downstream outage");
        WebhookEvent savedDead = webhookEventRepository.saveAndFlush(deadEvent);

        WebhookEvent completedEvent = new WebhookEvent("evt_comp_1", "shopify", "order.paid", "{}", WebhookStatus.COMPLETED);
        webhookEventRepository.saveAndFlush(completedEvent);

        // List endpoint returns only DEAD_LETTER
        Page<DeadLetterSummaryResponse> summaries = deadLetterService.listDeadLetters(PageRequest.of(0, 10));
        assertThat(summaries.getTotalElements()).isEqualTo(1);
        assertThat(summaries.getContent().get(0).id()).isEqualTo(savedDead.getId());
        assertThat(summaries.getContent().get(0).status()).isEqualTo(WebhookStatus.DEAD_LETTER);

        // Record attempts
        WebhookAttempt a1 = new WebhookAttempt(savedDead, 1, WebhookStatus.FAILED);
        a1.setErrorMessage("Attempt 1 error");
        webhookAttemptRepository.saveAndFlush(a1);

        WebhookAttempt a2 = new WebhookAttempt(savedDead, 2, WebhookStatus.FAILED);
        a2.setErrorMessage("Attempt 2 error");
        webhookAttemptRepository.saveAndFlush(a2);

        // Inspect endpoint returns full details and attempts in order
        DeadLetterDetailResponse detail = deadLetterService.inspectDeadLetter(savedDead.getId());
        assertThat(detail.id()).isEqualTo(savedDead.getId());
        assertThat(detail.attempts()).hasSize(2);
        assertThat(detail.attempts().get(0).attemptNumber()).isEqualTo(1);
        assertThat(detail.attempts().get(1).attemptNumber()).isEqualTo(2);

        // Inspect on non-dead-letter throws 409
        assertThatThrownBy(() -> deadLetterService.inspectDeadLetter(completedEvent.getId()))
            .isInstanceOf(InvalidStateTransitionException.class);
    }

    @Test
    @DisplayName("Replay transitions DEAD_LETTER event to RECEIVED, creates outbox record, preserves attempt history, and subsequent processing increments attempt number")
    void shouldReplayDeadLetterAndProcessSuccessfully() {
        WebhookEvent deadEvent = new WebhookEvent("evt_replay_success", "stripe", "charge.refunded", "{\"refund_id\":\"rf_1\"}", WebhookStatus.DEAD_LETTER);
        deadEvent.setAttemptCount(3);
        deadEvent.setLastError("Previous failure");
        WebhookEvent savedDead = webhookEventRepository.saveAndFlush(deadEvent);

        // Create historical attempts
        webhookAttemptRepository.saveAndFlush(new WebhookAttempt(savedDead, 1, WebhookStatus.FAILED));
        webhookAttemptRepository.saveAndFlush(new WebhookAttempt(savedDead, 2, WebhookStatus.FAILED));
        webhookAttemptRepository.saveAndFlush(new WebhookAttempt(savedDead, 3, WebhookStatus.FAILED));

        // Operator replays event
        WebhookReplayResponsePayload replayResponse = deadLetterService.replayDeadLetter(savedDead.getId(), "Manual resolution applied");
        assertThat(replayResponse.status()).isEqualTo(WebhookStatus.RECEIVED);

        // Verify database state after replay
        WebhookEvent afterReplay = webhookEventRepository.findById(savedDead.getId()).orElseThrow();
        assertThat(afterReplay.getStatus()).isEqualTo(WebhookStatus.RECEIVED);
        assertThat(afterReplay.getAttemptCount()).isEqualTo(3); // Preserved
        assertThat(afterReplay.getNextRetryAt()).isNull();

        // Verify outbox record created in PENDING
        List<OutboxEvent> outboxList = outboxEventRepository.findAll();
        assertThat(outboxList).hasSize(1);
        assertThat(outboxList.get(0).getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxList.get(0).getAggregateId()).isEqualTo(savedDead.getId().toString());

        // Verify replay audit recorded
        List<WebhookReplay> replays = webhookReplayRepository.findByWebhookEventOrderByRequestedAtDesc(afterReplay);
        assertThat(replays).hasSize(1);
        assertThat(replays.get(0).getReason()).isEqualTo("Manual resolution applied");
        assertThat(replays.get(0).getPreviousAttemptCount()).isEqualTo(3);

        // Worker simulates processing the replayed event
        doNothing().when(webhookEventProcessor).processEvent(any());
        webhookProcessingService.processEventById(savedDead.getId());

        // Verify next execution used attempt number 4 and succeeded
        WebhookEvent completed = webhookEventRepository.findById(savedDead.getId()).orElseThrow();
        assertThat(completed.getStatus()).isEqualTo(WebhookStatus.COMPLETED);
        assertThat(completed.getAttemptCount()).isEqualTo(4);

        List<WebhookAttempt> allAttempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(completed);
        assertThat(allAttempts).hasSize(4);
        assertThat(allAttempts.get(3).getAttemptNumber()).isEqualTo(4);
        assertThat(allAttempts.get(3).getStatus()).isEqualTo(WebhookStatus.COMPLETED);
    }

    @Test
    @DisplayName("Replaying a non-dead-letter event throws 409 and creates no outbox record")
    void shouldRejectReplayForNonDeadLetterEvent() {
        WebhookEvent processingEvent = new WebhookEvent("evt_proc", "stripe", "charge.succeeded", "{}", WebhookStatus.PROCESSING);
        WebhookEvent saved = webhookEventRepository.saveAndFlush(processingEvent);

        assertThatThrownBy(() -> deadLetterService.replayDeadLetter(saved.getId(), "Try replay"))
            .isInstanceOf(InvalidStateTransitionException.class);

        assertThat(outboxEventRepository.findAll()).isEmpty();
        assertThat(webhookReplayRepository.findAll()).isEmpty();
    }

    @Test
    @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    @DisplayName("Concurrent replay requests create at most one replay outbox record")
    void shouldPreventConcurrentDuplicateReplayJobs() throws InterruptedException {
        WebhookEvent deadEvent = new WebhookEvent("evt_concurrent_dlq", "stripe", "charge.failed", "{}", WebhookStatus.DEAD_LETTER);
        WebhookEvent saved = webhookEventRepository.saveAndFlush(deadEvent);

        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);

        AtomicInteger unexpectedErrorCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    deadLetterService.replayDeadLetter(saved.getId(), "Concurrent trigger");
                    successCount.incrementAndGet();
                } catch (InvalidStateTransitionException e) {
                    conflictCount.incrementAndGet();
                } catch (Throwable e) {
                    unexpectedErrorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        doneLatch.await(5, TimeUnit.SECONDS);

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(conflictCount.get()).isEqualTo(threadCount - 1);
        assertThat(outboxEventRepository.count()).isEqualTo(1);
        assertThat(webhookReplayRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Replayed event that fails again transitions according to retry policy")
    void shouldFollowRetryPolicyWhenReplayedEventFailsAgain() {
        WebhookEvent deadEvent = new WebhookEvent("evt_replay_fails", "stripe", "charge.failed", "{}", WebhookStatus.DEAD_LETTER);
        deadEvent.setAttemptCount(3);
        WebhookEvent saved = webhookEventRepository.saveAndFlush(deadEvent);

        deadLetterService.replayDeadLetter(saved.getId(), "Try again");

        // Processor fails on attempt 4
        doThrow(new RuntimeException("Still failing"))
            .when(webhookEventProcessor).processEvent(any());

        assertThatThrownBy(() -> webhookProcessingService.processEventById(saved.getId()))
            .isInstanceOf(RuntimeException.class);

        // Attempt count is 4. Because 4 >= maxAttempts (3), it immediately exhausts back to DEAD_LETTER!
        WebhookEvent afterFailure = webhookEventRepository.findById(saved.getId()).orElseThrow();
        assertThat(afterFailure.getStatus()).isEqualTo(WebhookStatus.DEAD_LETTER);
        assertThat(afterFailure.getAttemptCount()).isEqualTo(4);
        assertThat(afterFailure.getNextRetryAt()).isNull();

        List<WebhookAttempt> attempts = webhookAttemptRepository.findByWebhookEventOrderByAttemptNumberAsc(afterFailure);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getAttemptNumber()).isEqualTo(4);
        assertThat(attempts.get(0).getStatus()).isEqualTo(WebhookStatus.FAILED);
    }
}
