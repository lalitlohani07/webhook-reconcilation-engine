package com.example.webhookengine.controller;

import com.example.webhookengine.controller.dto.ErrorResponse;
import com.example.webhookengine.controller.dto.WebhookIngestRequest;
import com.example.webhookengine.controller.dto.WebhookIngestResponse;
import com.example.webhookengine.service.IngestionResult;
import com.example.webhookengine.service.WebhookIngestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/webhooks")
@Tag(name = "Webhooks", description = "Endpoints for webhook ingestion and lifecycle management")
public class WebhookController {

    private final WebhookIngestionService webhookIngestionService;

    public WebhookController(WebhookIngestionService webhookIngestionService) {
        this.webhookIngestionService = webhookIngestionService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
        summary = "Ingest incoming webhook event",
        description = "Receives a webhook event, validates the request, prevents duplicates via idempotency on (source, eventId), and persists it with RECEIVED status."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "202",
            description = "Webhook accepted for processing (new event)",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = WebhookIngestResponse.class))
        ),
        @ApiResponse(
            responseCode = "200",
            description = "Webhook event already received (duplicate event)",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = WebhookIngestResponse.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Bad request due to missing or invalid fields, or malformed JSON",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error during event ingestion",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<WebhookIngestResponse> ingestWebhook(@Valid @RequestBody WebhookIngestRequest request) {
        IngestionResult result = webhookIngestionService.ingestWebhook(request);

        if (result.isNew()) {
            return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(WebhookIngestResponse.accepted(
                    result.event().getEventId(),
                    result.event().getSource(),
                    result.event().getStatus()
                ));
        } else {
            return ResponseEntity
                .status(HttpStatus.OK)
                .body(WebhookIngestResponse.duplicate(
                    result.event().getEventId(),
                    result.event().getSource(),
                    result.event().getStatus()
                ));
        }
    }
}
