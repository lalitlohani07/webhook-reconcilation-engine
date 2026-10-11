package com.example.webhookengine.integration;

import com.example.webhookengine.config.TestFlywayConfig;
import com.example.webhookengine.controller.WebhookController;
import com.example.webhookengine.controller.dto.WebhookIngestRequest;
import com.example.webhookengine.exception.GlobalExceptionHandler;
import com.example.webhookengine.exception.RateLimiterUnavailableException;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import com.example.webhookengine.ratelimit.service.RateLimiterService;
import com.example.webhookengine.repository.OutboxEventRepository;
import com.example.webhookengine.repository.WebhookEventRepository;
import com.example.webhookengine.security.config.SecurityConfig;
import com.example.webhookengine.security.handler.RestAccessDeniedHandler;
import com.example.webhookengine.security.handler.RestAuthenticationEntryPoint;
import com.example.webhookengine.security.service.CustomUserDetailsService;
import com.example.webhookengine.service.WebhookIngestionService;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
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
    "spring.datasource.url=jdbc:h2:mem:ratelimit_ingest_test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "management.health.redis.enabled=false",
    "management.health.rabbit.enabled=false",
    "app.outbox.enabled=false",
    "app.retry.enabled=false"
})
class WebhookRateLimitIngestionIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private WebhookEventRepository webhookEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private RateLimiterService rateLimiterService;

    @MockitoBean
    private org.springframework.amqp.rabbit.connection.ConnectionFactory rabbitConnectionFactory;

    @MockitoBean
    private org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory redisConnectionFactory;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
        webhookEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Accepted webhook request within rate limit persists WebhookEvent and OutboxEvent and is idempotent")
    void shouldPersistWhenRateLimitAllowedAndMaintainIdempotency() throws Exception {
        when(rateLimiterService.tryAcquire(eq("stripe")))
            .thenReturn(RateLimitResult.allowed(60, 59));

        String requestJson = """
            {
                "eventId": "evt_rl_001",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 1000}
            }
            """;

        // First request (new) -> 202 Accepted
        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isAccepted())
            .andExpect(header().string("X-RateLimit-Limit", "60"))
            .andExpect(header().string("X-RateLimit-Remaining", "59"));

        assertThat(webhookEventRepository.count()).isEqualTo(1);
        assertThat(outboxEventRepository.count()).isEqualTo(1);

        // Duplicate request within rate limit -> 200 OK (idempotent, no duplicate records)
        when(rateLimiterService.tryAcquire(eq("stripe")))
            .thenReturn(RateLimitResult.allowed(60, 58));

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isOk())
            .andExpect(header().string("X-RateLimit-Limit", "60"))
            .andExpect(header().string("X-RateLimit-Remaining", "58"));

        assertThat(webhookEventRepository.count()).isEqualTo(1);
        assertThat(outboxEventRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Rejected rate-limited request returns 429 and does not persist WebhookEvent or OutboxEvent")
    void shouldNotPersistWhenRateLimitExceeded() throws Exception {
        when(rateLimiterService.tryAcquire(eq("stripe")))
            .thenReturn(RateLimitResult.denied(60, 50));

        String requestJson = """
            {
                "eventId": "evt_rl_blocked",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 1000}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isTooManyRequests())
            .andExpect(header().string("Retry-After", "50"))
            .andExpect(header().string("X-RateLimit-Limit", "60"))
            .andExpect(header().string("X-RateLimit-Remaining", "0"))
            .andExpect(jsonPath("$.status").value(429))
            .andExpect(jsonPath("$.error").value("Too Many Requests"));

        // Crucial guarantee: no database persistence occurs
        assertThat(webhookEventRepository.count()).isZero();
        assertThat(outboxEventRepository.count()).isZero();
    }

    @Test
    @DisplayName("When Redis rate limiter is unavailable (fail-closed), returns 503 and does not persist records")
    void shouldNotPersistWhenRateLimiterUnavailable() throws Exception {
        when(rateLimiterService.tryAcquire(eq("stripe")))
            .thenThrow(new RateLimiterUnavailableException("Redis cluster unreachable"));

        String requestJson = """
            {
                "eventId": "evt_rl_down",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 1000}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isServiceUnavailable())
            .andExpect(header().string("Retry-After", "30"))
            .andExpect(jsonPath("$.status").value(503))
            .andExpect(jsonPath("$.error").value("Service Unavailable"));

        assertThat(webhookEventRepository.count()).isZero();
        assertThat(outboxEventRepository.count()).isZero();
    }
}
