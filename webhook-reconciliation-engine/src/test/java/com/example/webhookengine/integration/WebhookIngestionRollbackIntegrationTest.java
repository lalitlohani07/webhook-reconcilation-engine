package com.example.webhookengine.integration;

import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.service.WebhookIngestionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@DataJpaTest
@Import({TestFlywayConfig.class, WebhookIngestionService.class, ObjectMapper.class})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.url=jdbc:h2:mem:webhook_rollback_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver"
})
class WebhookIngestionRollbackIntegrationTest {

    @Autowired
    private WebhookIngestionService webhookIngestionService;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @MockitoBean
    private OutboxEventRepository outboxEventRepository;

    @Test
    @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    @DisplayName("Transaction rollback: if Outbox persistence fails, WebhookEvent is also rolled back")
    void shouldRollbackWebhookEventIfOutboxFails() {
        WebhookEvent event = new WebhookEvent(
            "evt_rollback",
            "stripe",
            "payment.failed",
            "{\"amount\":100}",
            WebhookStatus.RECEIVED
        );

        // When outboxEventRepository throws an exception during persistEventAndOutbox
        doThrow(new RuntimeException("Simulated Outbox Disk Error"))
            .when(outboxEventRepository).saveAndFlush(any());

        assertThatThrownBy(() -> webhookIngestionService.persistEventAndOutbox(event))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Simulated Outbox Disk Error");

        // Verify that the webhook event was rolled back and is not in the database
        assertThat(webhookEventRepository.findBySourceAndEventId("stripe", "evt_rollback")).isEmpty();
    }
}
