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
import com.example.webhookengine.exception.RateLimitExceededException;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import com.example.webhookengine.ratelimit.service.RateLimiterService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/webhooks")
@Tag(name = "Webhooks", description = "Public endpoints for webhook ingestion, protected by Redis rate limiting")
public class WebhookController {

    private final WebhookIngestionService webhookIngestionService;
    private final RateLimiterService rateLimiterService;

    public WebhookController(WebhookIngestionService webhookIngestionService, RateLimiterService rateLimiterService) {
        this.webhookIngestionService = webhookIngestionService;
        this.rateLimiterService = rateLimiterService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
        summary = "Ingest incoming webhook event",
        description = "Receives a webhook event, validates the request, enforces Redis rate limiting, prevents duplicates via idempotency on (source, eventId), and persists it with RECEIVED status."
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
            responseCode = "429",
            description = "Too many requests - rate limit exceeded for webhook source",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "503",
            description = "Service unavailable - rate limiting infrastructure temporarily down",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error during event ingestion",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<WebhookIngestResponse> ingestWebhook(@Valid @RequestBody WebhookIngestRequest request) {
        RateLimitResult rateLimitResult = rateLimiterService.tryAcquire(request.getSource());
        if (!rateLimitResult.allowed()) {
            throw new RateLimitExceededException(
                rateLimitResult.limit(),
                rateLimitResult.remaining(),
                rateLimitResult.retryAfterSeconds(),
                request.getSource()
            );
        }

        IngestionResult result = webhookIngestionService.ingestWebhook(request);

        HttpStatus status = result.isNew() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        WebhookIngestResponse body = result.isNew()
            ? WebhookIngestResponse.accepted(result.event().getEventId(), result.event().getSource(), result.event().getStatus())
            : WebhookIngestResponse.duplicate(result.event().getEventId(), result.event().getSource(), result.event().getStatus());

        return ResponseEntity
            .status(status)
            .header("X-RateLimit-Limit", String.valueOf(rateLimitResult.limit()))
            .header("X-RateLimit-Remaining", String.valueOf(rateLimitResult.remaining()))
            .body(body);
    }
}
