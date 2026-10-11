package com.example.webhookengine.security;

import com.example.webhookengine.controller.DeadLetterController;
import com.example.webhookengine.controller.WebhookController;
import com.example.webhookengine.controller.dto.DeadLetterDetailResponse;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.GlobalExceptionHandler;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import com.example.webhookengine.ratelimit.service.RateLimiterService;
import com.example.webhookengine.security.config.SecurityConfig;
import com.example.webhookengine.security.domain.SecurityRole;
import com.example.webhookengine.security.domain.SecurityUser;
import com.example.webhookengine.security.handler.RestAccessDeniedHandler;
import com.example.webhookengine.security.handler.RestAuthenticationEntryPoint;
import com.example.webhookengine.security.repository.SecurityUserRepository;
import com.example.webhookengine.security.service.CustomUserDetailsService;
import com.example.webhookengine.service.DeadLetterService;
import com.example.webhookengine.service.WebhookIngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = {DeadLetterController.class, WebhookController.class})
@Import({
    SecurityConfig.class,
    CustomUserDetailsService.class,
    RestAuthenticationEntryPoint.class,
    RestAccessDeniedHandler.class,
    GlobalExceptionHandler.class
})
class SecurityIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private SecurityUserRepository securityUserRepository;

    @MockitoBean
    private DeadLetterService deadLetterService;

    @MockitoBean
    private WebhookIngestionService webhookIngestionService;

    @MockitoBean
    private RateLimiterService rateLimiterService;

    private final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventId = UUID.randomUUID();

        // Admin User
        SecurityUser admin = new SecurityUser(
            UUID.randomUUID(),
            "admin_user",
            passwordEncoder.encode("adminSecret123!"),
            SecurityRole.ADMIN,
            true
        );

        // Operator User
        SecurityUser operator = new SecurityUser(
            UUID.randomUUID(),
            "operator_user",
            passwordEncoder.encode("operatorSecret123!"),
            SecurityRole.OPERATOR,
            true
        );

        // Viewer User
        SecurityUser viewer = new SecurityUser(
            UUID.randomUUID(),
            "viewer_user",
            passwordEncoder.encode("viewerSecret123!"),
            SecurityRole.VIEWER,
            true
        );

        // Disabled User
        SecurityUser disabledUser = new SecurityUser(
            UUID.randomUUID(),
            "disabled_user",
            passwordEncoder.encode("disabledSecret123!"),
            SecurityRole.OPERATOR,
            false
        );

        when(securityUserRepository.findByUsername("admin_user")).thenReturn(Optional.of(admin));
        when(securityUserRepository.findByUsername("operator_user")).thenReturn(Optional.of(operator));
        when(securityUserRepository.findByUsername("viewer_user")).thenReturn(Optional.of(viewer));
        when(securityUserRepository.findByUsername("disabled_user")).thenReturn(Optional.of(disabledUser));

        when(rateLimiterService.tryAcquire(any())).thenReturn(RateLimitResult.allowed(60, 59));
    }

    private String basicAuth(String username, String password) {
        String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Unauthenticated request to dead-letters list endpoint returns 401 Unauthorized")
    void shouldReturn401WhenUnauthenticatedOnList() throws Exception {
        mockMvc.perform(get("/api/v1/dead-letters"))
            .andExpect(status().isUnauthorized())
            .andExpect(header().string("WWW-Authenticate", "Basic realm=\"Webhook Reconciliation Engine\""))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("Unauthenticated request to dead-letter replay endpoint returns 401 Unauthorized")
    void shouldReturn401WhenUnauthenticatedOnReplay() throws Exception {
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isUnauthorized())
            .andExpect(header().string("WWW-Authenticate", "Basic realm=\"Webhook Reconciliation Engine\""))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("Invalid credentials return 401 Unauthorized")
    void shouldReturn401WhenCredentialsAreInvalid() throws Exception {
        mockMvc.perform(get("/api/v1/dead-letters")
                .header("Authorization", basicAuth("admin_user", "wrong_password")))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("Disabled user account returns 401 Unauthorized")
    void shouldReturn401WhenUserAccountIsDisabled() throws Exception {
        mockMvc.perform(get("/api/v1/dead-letters")
                .header("Authorization", basicAuth("disabled_user", "disabledSecret123!")))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("Authenticated ADMIN can inspect and replay a dead-letter event")
    void shouldAllowAdminToInspectAndReplay() throws Exception {
        DeadLetterDetailResponse detail = new DeadLetterDetailResponse(
            eventId, "evt_001", "stripe", "charge.failed", WebhookStatus.DEAD_LETTER,
            1, "Error", Instant.now(), Instant.now(), null, List.of(), List.of()
        );
        when(deadLetterService.inspectDeadLetter(eventId)).thenReturn(detail);

        WebhookReplayResponsePayload replayResponse = new WebhookReplayResponsePayload(
            eventId, "evt_001", "stripe", WebhookStatus.RECEIVED, "Replayed"
        );
        when(deadLetterService.replayDeadLetter(eq(eventId), any())).thenReturn(replayResponse);

        // Inspect
        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .header("Authorization", basicAuth("admin_user", "adminSecret123!")))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(eventId.toString()));

        // Replay
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .header("Authorization", basicAuth("admin_user", "adminSecret123!"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.status").value("RECEIVED"));
    }

    @Test
    @DisplayName("Authenticated OPERATOR can inspect and replay a dead-letter event")
    void shouldAllowOperatorToInspectAndReplay() throws Exception {
        DeadLetterDetailResponse detail = new DeadLetterDetailResponse(
            eventId, "evt_001", "stripe", "charge.failed", WebhookStatus.DEAD_LETTER,
            1, "Error", Instant.now(), Instant.now(), null, List.of(), List.of()
        );
        when(deadLetterService.inspectDeadLetter(eventId)).thenReturn(detail);

        WebhookReplayResponsePayload replayResponse = new WebhookReplayResponsePayload(
            eventId, "evt_001", "stripe", WebhookStatus.RECEIVED, "Replayed"
        );
        when(deadLetterService.replayDeadLetter(eq(eventId), any())).thenReturn(replayResponse);

        // Inspect
        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .header("Authorization", basicAuth("operator_user", "operatorSecret123!")))
            .andExpect(status().isOk());

        // Replay
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .header("Authorization", basicAuth("operator_user", "operatorSecret123!"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.status").value("RECEIVED"));
    }

    @Test
    @DisplayName("Authenticated VIEWER can inspect dead-letter event but is denied replay with 403 Forbidden")
    void shouldAllowViewerToInspectButDenyReplay() throws Exception {
        DeadLetterDetailResponse detail = new DeadLetterDetailResponse(
            eventId, "evt_001", "stripe", "charge.failed", WebhookStatus.DEAD_LETTER,
            1, "Error", Instant.now(), Instant.now(), null, List.of(), List.of()
        );
        when(deadLetterService.inspectDeadLetter(eventId)).thenReturn(detail);

        // Inspect allowed for VIEWER
        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .header("Authorization", basicAuth("viewer_user", "viewerSecret123!")))
            .andExpect(status().isOk());

        // Replay denied with 403 Forbidden for VIEWER
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .header("Authorization", basicAuth("viewer_user", "viewerSecret123!"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isForbidden())
            .andExpect(jsonPath("$.status").value(403))
            .andExpect(jsonPath("$.error").value("Forbidden"))
            .andExpect(jsonPath("$.message").value("Access is denied: insufficient permissions for this resource"));
    }

    @Test
    @DisplayName("Public webhook ingestion endpoint remains accessible unauthenticated")
    void shouldAllowUnauthenticatedPublicWebhookIngestion() throws Exception {
        String requestJson = """
            {
                "eventId": "evt_public_001",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 500}
            }
            """;

        when(webhookIngestionService.ingestWebhook(any()))
            .thenReturn(new com.example.webhookengine.service.IngestionResult(
                new com.example.webhookengine.domain.WebhookEvent(
                    "evt_public_001", "stripe", "payment.succeeded", "{\"amount\": 500}", WebhookStatus.RECEIVED
                ), true
            ));

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isAccepted())
            .andExpect(header().exists("X-RateLimit-Limit"))
            .andExpect(header().exists("X-RateLimit-Remaining"));
    }
}
