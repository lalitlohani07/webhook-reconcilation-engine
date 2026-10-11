package com.example.webhookengine.controller;

import com.example.webhookengine.controller.dto.ReconciliationRecordDetailResponse;
import com.example.webhookengine.controller.dto.ReconciliationRecordSummaryResponse;
import com.example.webhookengine.controller.dto.ReconciliationRunRequest;
import com.example.webhookengine.controller.dto.ReconciliationRunResponse;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.exception.GlobalExceptionHandler;
import com.example.webhookengine.exception.ResourceNotFoundException;
import com.example.webhookengine.reconciliation.service.ReconciliationService;
import com.example.webhookengine.security.config.SecurityConfig;
import com.example.webhookengine.security.handler.RestAccessDeniedHandler;
import com.example.webhookengine.security.handler.RestAuthenticationEntryPoint;
import com.example.webhookengine.security.service.CustomUserDetailsService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ReconciliationController.class)
@Import({
    SecurityConfig.class,
    RestAuthenticationEntryPoint.class,
    RestAccessDeniedHandler.class,
    GlobalExceptionHandler.class
})
class ReconciliationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private ReconciliationService reconciliationService;

    @MockitoBean
    private CustomUserDetailsService userDetailsService;

    // --- Authentication (401) Tests ---

    @Test
    @DisplayName("POST /api/v1/reconciliation/runs returns 401 Unauthorized when unauthenticated")
    void shouldReturn401WhenUnauthenticatedOnRunReconciliation() throws Exception {
        mockMvc.perform(post("/api/v1/reconciliation/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"batchSize\": 25}"))
            .andExpect(status().isUnauthorized())
            .andExpect(header().exists("WWW-Authenticate"))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("GET /api/v1/reconciliation/records returns 401 Unauthorized when unauthenticated")
    void shouldReturn401WhenUnauthenticatedOnListRecords() throws Exception {
        mockMvc.perform(get("/api/v1/reconciliation/records")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isUnauthorized())
            .andExpect(header().exists("WWW-Authenticate"))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("GET /api/v1/reconciliation/records/{id} returns 401 Unauthorized when unauthenticated")
    void shouldReturn401WhenUnauthenticatedOnGetRecord() throws Exception {
        mockMvc.perform(get("/api/v1/reconciliation/records/{id}", UUID.randomUUID())
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isUnauthorized())
            .andExpect(header().exists("WWW-Authenticate"))
            .andExpect(jsonPath("$.status").value(401))
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    // --- Authorization (403 vs 200) Tests ---

    @Test
    @WithMockUser(roles = "VIEWER")
    @DisplayName("POST /api/v1/reconciliation/runs returns 403 Forbidden for VIEWER role")
    void shouldReturn403WhenViewerTriesToRunReconciliation() throws Exception {
        mockMvc.perform(post("/api/v1/reconciliation/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"batchSize\": 25}"))
            .andExpect(status().isForbidden())
            .andExpect(jsonPath("$.status").value(403))
            .andExpect(jsonPath("$.error").value("Forbidden"))
            .andExpect(jsonPath("$.message").value("Access is denied: insufficient permissions for this resource"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/reconciliation/runs returns 200 OK for OPERATOR role")
    void shouldAllowOperatorToRunReconciliation() throws Exception {
        UUID runId = UUID.randomUUID();
        ReconciliationRunResponse mockResponse = new ReconciliationRunResponse(
            runId, Instant.now(), Instant.now(), 5, 3, 1, 1, 0
        );
        when(reconciliationService.runReconciliation(any(ReconciliationRunRequest.class)))
            .thenReturn(mockResponse);

        mockMvc.perform(post("/api/v1/reconciliation/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"batchSize\": 50}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.runId").value(runId.toString()))
            .andExpect(jsonPath("$.totalEvaluated").value(5))
            .andExpect(jsonPath("$.matchedCount").value(3))
            .andExpect(jsonPath("$.correctedCount").value(1))
            .andExpect(jsonPath("$.mismatchCount").value(1))
            .andExpect(jsonPath("$.failedCount").value(0));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("POST /api/v1/reconciliation/runs returns 200 OK for ADMIN role")
    void shouldAllowAdminToRunReconciliation() throws Exception {
        UUID runId = UUID.randomUUID();
        ReconciliationRunResponse mockResponse = new ReconciliationRunResponse(
            runId, Instant.now(), Instant.now(), 2, 2, 0, 0, 0
        );
        when(reconciliationService.runReconciliation(any(ReconciliationRunRequest.class)))
            .thenReturn(mockResponse);

        mockMvc.perform(post("/api/v1/reconciliation/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"batchSize\": 20}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.runId").value(runId.toString()))
            .andExpect(jsonPath("$.totalEvaluated").value(2));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("POST /api/v1/reconciliation/runs returns 400 Bad Request when batchSize is invalid")
    void shouldReturn400WhenBatchSizeIsInvalid() throws Exception {
        mockMvc.perform(post("/api/v1/reconciliation/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"batchSize\": 0}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Validation Failed"));

        mockMvc.perform(post("/api/v1/reconciliation/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"batchSize\": 101}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Validation Failed"));
    }

    @Test
    @WithMockUser(roles = "VIEWER")
    @DisplayName("GET /api/v1/reconciliation/records returns 200 OK for VIEWER role with filters")
    void shouldAllowViewerToListRecordsWithFilters() throws Exception {
        UUID recordId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ReconciliationRecordSummaryResponse summary = new ReconciliationRecordSummaryResponse(
            recordId, eventId, "evt-123", "stripe", "PAID", "COMPLETED", ReconciliationResult.MATCHED, Instant.now()
        );

        when(reconciliationService.listRecords(eq(ReconciliationResult.MATCHED), eq("stripe"), any(Pageable.class)))
            .thenReturn(new PageImpl<>(List.of(summary)));

        mockMvc.perform(get("/api/v1/reconciliation/records")
                .param("result", "MATCHED")
                .param("source", "stripe")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.content[0].id").value(recordId.toString()))
            .andExpect(jsonPath("$.content[0].webhookEventId").value(eventId.toString()))
            .andExpect(jsonPath("$.content[0].eventId").value("evt-123"))
            .andExpect(jsonPath("$.content[0].source").value("stripe"))
            .andExpect(jsonPath("$.content[0].result").value("MATCHED"));

        verify(reconciliationService).listRecords(eq(ReconciliationResult.MATCHED), eq("stripe"), any(Pageable.class));
    }

    @Test
    @WithMockUser(roles = "VIEWER")
    @DisplayName("GET /api/v1/reconciliation/records/{id} returns 200 OK for VIEWER role")
    void shouldAllowViewerToInspectRecord() throws Exception {
        UUID recordId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ReconciliationRecordDetailResponse detail = new ReconciliationRecordDetailResponse(
            recordId,
            eventId,
            "evt-abc",
            "stripe",
            "payment.captured",
            "PAID",
            "COMPLETED",
            ReconciliationResult.MATCHED,
            Instant.now(),
            objectMapper.readTree("{\"classification\":\"MATCHED\"}")
        );

        when(reconciliationService.getRecord(recordId)).thenReturn(detail);

        mockMvc.perform(get("/api/v1/reconciliation/records/{id}", recordId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(recordId.toString()))
            .andExpect(jsonPath("$.eventId").value("evt-abc"))
            .andExpect(jsonPath("$.details.classification").value("MATCHED"));
    }

    @Test
    @WithMockUser(roles = "OPERATOR")
    @DisplayName("GET /api/v1/reconciliation/records/{id} returns 404 Not Found when ID does not exist")
    void shouldReturn404WhenRecordDoesNotExist() throws Exception {
        UUID nonExistentId = UUID.randomUUID();
        when(reconciliationService.getRecord(nonExistentId))
            .thenThrow(new ResourceNotFoundException("Reconciliation record not found with id: " + nonExistentId));

        mockMvc.perform(get("/api/v1/reconciliation/records/{id}", nonExistentId)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.status").value(404))
            .andExpect(jsonPath("$.error").value("Not Found"))
            .andExpect(jsonPath("$.message").value("Reconciliation record not found with id: " + nonExistentId));
    }
}
