package com.example.webhookengine.controller;

import com.example.webhookengine.controller.dto.DeadLetterDetailResponse;
import com.example.webhookengine.controller.dto.DeadLetterSummaryResponse;
import com.example.webhookengine.controller.dto.WebhookAttemptResponse;
import com.example.webhookengine.controller.dto.WebhookReplayRequest;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.GlobalExceptionHandler;
import com.example.webhookengine.exception.InvalidStateTransitionException;
import com.example.webhookengine.exception.ResourceNotFoundException;
import com.example.webhookengine.security.config.SecurityConfig;
import com.example.webhookengine.security.handler.RestAccessDeniedHandler;
import com.example.webhookengine.security.handler.RestAuthenticationEntryPoint;
import com.example.webhookengine.security.service.CustomUserDetailsService;
import com.example.webhookengine.service.DeadLetterService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DeadLetterController.class)
@Import({
    SecurityConfig.class,
    RestAuthenticationEntryPoint.class,
    RestAccessDeniedHandler.class,
    GlobalExceptionHandler.class
})
class DeadLetterControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private DeadLetterService deadLetterService;

    @MockitoBean
    private CustomUserDetailsService userDetailsService;

    private UUID eventId;

    @BeforeEach
    void setUp() {
        eventId = UUID.randomUUID();
    }

    // --- Authentication & Authorization Security Tests ---

    @Test
    @DisplayName("GET /api/v1/dead-letters returns 401 Unauthorized when unauthenticated")
    void shouldReturn401WhenUnauthenticatedOnListDeadLetters() throws Exception {
        mockMvc.perform(get("/api/v1/dead-letters")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isUnauthorized())
            .andExpect(header().exists("WWW-Authenticate"))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"))
            .andExpect(jsonPath("$.message").value("Full authentication is required to access this resource"));
    }

    @Test
    @DisplayName("GET /api/v1/dead-letters/{id} returns 401 Unauthorized when unauthenticated")
    void shouldReturn401WhenUnauthenticatedOnInspectDeadLetter() throws Exception {
        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isUnauthorized())
            .andExpect(header().exists("WWW-Authenticate"))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 401 Unauthorized when unauthenticated")
    void shouldReturn401WhenUnauthenticatedOnReplayDeadLetter() throws Exception {
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isUnauthorized())
            .andExpect(header().exists("WWW-Authenticate"))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @WithMockUser(roles = "VIEWER")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 403 Forbidden when VIEWER attempts replay")
    void shouldReturn403WhenViewerAttemptsReplay() throws Exception {
        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isForbidden())
            .andExpect(jsonPath("$.status").value(403))
            .andExpect(jsonPath("$.error").value("Forbidden"))
            .andExpect(jsonPath("$.message").value("Access is denied: insufficient permissions for this resource"));
    }

    @Test
    @WithMockUser(roles = "VIEWER")
    @DisplayName("GET /api/v1/dead-letters returns 200 OK when authenticated as VIEWER")
    void shouldAllowViewerToListDeadLetters() throws Exception {
        when(deadLetterService.listDeadLetters(any(Pageable.class)))
            .thenReturn(new PageImpl<>(List.of()));

        mockMvc.perform(get("/api/v1/dead-letters")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(roles = "VIEWER")
    @DisplayName("GET /api/v1/dead-letters/{id} returns 200 OK when authenticated as VIEWER")
    void shouldAllowViewerToInspectDeadLetter() throws Exception {
        DeadLetterDetailResponse detail = new DeadLetterDetailResponse(
            eventId, "evt_001", "stripe", "charge.failed", WebhookStatus.DEAD_LETTER,
            1, "Timeout", Instant.now(), Instant.now(), null, List.of(), List.of()
        );
        when(deadLetterService.inspectDeadLetter(eventId)).thenReturn(detail);

        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(eventId.toString()));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 202 Accepted when authenticated as OPERATOR")
    void shouldAllowOperatorToReplayDeadLetter() throws Exception {
        WebhookReplayResponsePayload responsePayload = new WebhookReplayResponsePayload(
            eventId, "evt_001", "stripe", WebhookStatus.RECEIVED, "Replay accepted"
        );
        when(deadLetterService.replayDeadLetter(eq(eventId), any())).thenReturn(responsePayload);

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.id").value(eventId.toString()))
            .andExpect(jsonPath("$.status").value("RECEIVED"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 202 Accepted when authenticated as ADMIN")
    void shouldAllowAdminToReplayDeadLetter() throws Exception {
        WebhookReplayResponsePayload responsePayload = new WebhookReplayResponsePayload(
            eventId, "evt_001", "stripe", WebhookStatus.RECEIVED, "Replay accepted"
        );
        when(deadLetterService.replayDeadLetter(eq(eventId), any())).thenReturn(responsePayload);

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.id").value(eventId.toString()))
            .andExpect(jsonPath("$.status").value("RECEIVED"));
    }

    // --- Business Logic Tests (under OPERATOR authorization) ---

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("GET /api/v1/dead-letters returns 200 with paginated list")
    void shouldListDeadLetters() throws Exception {
        DeadLetterSummaryResponse summary = new DeadLetterSummaryResponse(
            eventId,
            "evt_001",
            "stripe",
            "charge.failed",
            WebhookStatus.DEAD_LETTER,
            5,
            "Service unavailable",
            Instant.now(),
            Instant.now()
        );

        when(deadLetterService.listDeadLetters(any(Pageable.class)))
            .thenReturn(new PageImpl<>(List.of(summary)));

        mockMvc.perform(get("/api/v1/dead-letters")
                .param("page", "0")
                .param("size", "10")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.content[0].id").value(eventId.toString()))
            .andExpect(jsonPath("$.content[0].eventId").value("evt_001"))
            .andExpect(jsonPath("$.content[0].source").value("stripe"))
            .andExpect(jsonPath("$.content[0].status").value("DEAD_LETTER"))
            .andExpect(jsonPath("$.content[0].attemptCount").value(5))
            .andExpect(jsonPath("$.content[0].lastError").value("Service unavailable"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("GET /api/v1/dead-letters/{id} returns 200 with details and attempt history")
    void shouldInspectDeadLetter() throws Exception {
        WebhookAttemptResponse attempt = new WebhookAttemptResponse(
            UUID.randomUUID(),
            1,
            WebhookStatus.FAILED,
            Instant.now(),
            Instant.now(),
            "Timeout connecting to database"
        );

        DeadLetterDetailResponse detail = new DeadLetterDetailResponse(
            eventId,
            "evt_001",
            "stripe",
            "charge.failed",
            WebhookStatus.DEAD_LETTER,
            1,
            "Timeout connecting to database",
            Instant.now(),
            Instant.now(),
            null,
            List.of(attempt),
            List.of()
        );

        when(deadLetterService.inspectDeadLetter(eventId)).thenReturn(detail);

        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(eventId.toString()))
            .andExpect(jsonPath("$.status").value("DEAD_LETTER"))
            .andExpect(jsonPath("$.attempts[0].attemptNumber").value(1))
            .andExpect(jsonPath("$.attempts[0].errorMessage").value("Timeout connecting to database"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("GET /api/v1/dead-letters/{id} returns 404 when event is missing")
    void shouldReturn404WhenEventNotFound() throws Exception {
        when(deadLetterService.inspectDeadLetter(eventId))
            .thenThrow(new ResourceNotFoundException("Webhook event not found with ID: " + eventId));

        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.status").value(404))
            .andExpect(jsonPath("$.error").value("Not Found"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("GET /api/v1/dead-letters/{id} returns 409 when event is not in DEAD_LETTER status")
    void shouldReturn409WhenEventNotInDeadLetterStatus() throws Exception {
        when(deadLetterService.inspectDeadLetter(eventId))
            .thenThrow(new InvalidStateTransitionException("Webhook event with ID " + eventId + " is not in DEAD_LETTER state"));

        mockMvc.perform(get("/api/v1/dead-letters/{id}", eventId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.status").value(409))
            .andExpect(jsonPath("$.error").value("Conflict"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 202 Accepted on successful replay")
    void shouldReplayDeadLetterSuccessfully() throws Exception {
        WebhookReplayResponsePayload responsePayload = new WebhookReplayResponsePayload(
            eventId,
            "evt_001",
            "stripe",
            WebhookStatus.RECEIVED,
            "Webhook event replay accepted and enqueued for processing"
        );

        when(deadLetterService.replayDeadLetter(eq(eventId), eq("Issue resolved")))
            .thenReturn(responsePayload);

        WebhookReplayRequest request = new WebhookReplayRequest("Issue resolved");

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.id").value(eventId.toString()))
            .andExpect(jsonPath("$.eventId").value("evt_001"))
            .andExpect(jsonPath("$.status").value("RECEIVED"))
            .andExpect(jsonPath("$.message").value("Webhook event replay accepted and enqueued for processing"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 409 when event cannot be replayed")
    void shouldReturn409WhenReplayingNonDeadLetterEvent() throws Exception {
        when(deadLetterService.replayDeadLetter(eq(eventId), any()))
            .thenThrow(new InvalidStateTransitionException("Webhook event with ID " + eventId + " is not eligible for replay"));

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.status").value(409))
            .andExpect(jsonPath("$.error").value("Conflict"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 404 when event does not exist")
    void shouldReturn404WhenReplayingMissingEvent() throws Exception {
        when(deadLetterService.replayDeadLetter(eq(eventId), any()))
            .thenThrow(new ResourceNotFoundException("Webhook event not found with ID: " + eventId));

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/dead-letters/{id}/replay returns 400 when request body fails validation")
    void shouldReturn400WhenReplayRequestIsInvalid() throws Exception {
        String longReason = "A".repeat(501);
        String body = String.format("{\"reason\":\"%s\"}", longReason);

        mockMvc.perform(post("/api/v1/dead-letters/{id}/replay", eventId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Validation Failed"));
    }
}
