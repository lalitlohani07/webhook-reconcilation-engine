package com.example.webhookengine.controller;

import com.example.webhookengine.controller.dto.WebhookIngestRequest;
import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import com.example.webhookengine.exception.GlobalExceptionHandler;
import com.example.webhookengine.service.IngestionResult;
import com.example.webhookengine.service.WebhookIngestionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(WebhookController.class)
@Import(GlobalExceptionHandler.class)
class WebhookControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private WebhookIngestionService webhookIngestionService;

    @Test
    @DisplayName("POST /api/v1/webhooks returns 202 Accepted for new event")
    void shouldReturn202AcceptedForNewEvent() throws Exception {
        WebhookEvent savedEvent = new WebhookEvent("evt_001", "stripe", "payment.succeeded", "{\"amount\": 1000}", WebhookStatus.RECEIVED);
        when(webhookIngestionService.ingestWebhook(any(WebhookIngestRequest.class)))
            .thenReturn(new IngestionResult(savedEvent, true));

        String requestJson = """
            {
                "eventId": "evt_001",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 1000}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.eventId").value("evt_001"))
            .andExpect(jsonPath("$.source").value("stripe"))
            .andExpect(jsonPath("$.status").value("RECEIVED"))
            .andExpect(jsonPath("$.message").value("Webhook accepted for processing"));
    }

    @Test
    @DisplayName("POST /api/v1/webhooks returns 200 OK for duplicate event with existing status")
    void shouldReturn200OkForDuplicateEvent() throws Exception {
        WebhookEvent existingEvent = new WebhookEvent("evt_001", "stripe", "payment.succeeded", "{\"amount\": 1000}", WebhookStatus.PROCESSING);
        when(webhookIngestionService.ingestWebhook(any(WebhookIngestRequest.class)))
            .thenReturn(new IngestionResult(existingEvent, false));

        String requestJson = """
            {
                "eventId": "evt_001",
                "source": "stripe",
                "eventType": "payment.succeeded",
                "payload": {"amount": 1000}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.eventId").value("evt_001"))
            .andExpect(jsonPath("$.source").value("stripe"))
            .andExpect(jsonPath("$.status").value("PROCESSING"))
            .andExpect(jsonPath("$.message").value("Webhook event already received"));
    }

    @Test
    @DisplayName("POST /api/v1/webhooks returns 400 Bad Request when required fields are missing")
    void shouldReturn400BadRequestForMissingFields() throws Exception {
        String invalidRequestJson = """
            {
                "eventId": "",
                "source": "   ",
                "payload": null
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidRequestJson))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Validation Failed"))
            .andExpect(jsonPath("$.details").isArray());
    }

    @Test
    @DisplayName("POST /api/v1/webhooks returns 400 Bad Request for malformed JSON body")
    void shouldReturn400BadRequestForMalformedJson() throws Exception {
        String malformedJson = "{ eventId: 'evt_bad', invalidJson ";

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(malformedJson))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value(400))
            .andExpect(jsonPath("$.error").value("Malformed Request"));
    }

    @Test
    @DisplayName("POST /api/v1/webhooks returns 500 Internal Server Error on unexpected exception")
    void shouldReturn500OnUnexpectedException() throws Exception {
        when(webhookIngestionService.ingestWebhook(any(WebhookIngestRequest.class)))
            .thenThrow(new RuntimeException("Database down"));

        String requestJson = """
            {
                "eventId": "evt_crash",
                "source": "stripe",
                "eventType": "test",
                "payload": {"foo": "bar"}
            }
            """;

        mockMvc.perform(post("/api/v1/webhooks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestJson))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.status").value(500))
            .andExpect(jsonPath("$.error").value("Internal Server Error"))
            .andExpect(jsonPath("$.message").value("An unexpected error occurred while processing the request"));
    }
}
