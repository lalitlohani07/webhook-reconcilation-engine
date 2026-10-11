package com.example.webhookengine.controller;

import com.example.webhookengine.controller.dto.DeadLetterDetailResponse;
import com.example.webhookengine.controller.dto.DeadLetterSummaryResponse;
import com.example.webhookengine.controller.dto.ErrorResponse;
import com.example.webhookengine.controller.dto.WebhookReplayRequest;
import com.example.webhookengine.controller.dto.WebhookReplayResponsePayload;
import com.example.webhookengine.service.DeadLetterService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dead-letters")
@Tag(name = "Dead Letters", description = "Endpoints for inspecting and replaying dead-letter webhook events")
@SecurityRequirement(name = "basicAuth")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR', 'VIEWER')")
    @Operation(
        summary = "List dead-letter webhook events",
        description = "Retrieves a paginated list of webhook events that have exhausted retries and transitioned to DEAD_LETTER status. Requires ADMIN, OPERATOR, or VIEWER role."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Paginated list of dead-letter events",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)
        ),
        @ApiResponse(
            responseCode = "401",
            description = "Unauthorized - authentication credentials missing or invalid",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Forbidden - authenticated user lacks required role",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error while querying dead-letter events",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<Page<DeadLetterSummaryResponse>> listDeadLetters(
        @PageableDefault(size = 20, sort = "createdAt", direction = Sort.Direction.DESC) Pageable pageable
    ) {
        Page<DeadLetterSummaryResponse> result = deadLetterService.listDeadLetters(pageable);
        return ResponseEntity.ok(result);
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR', 'VIEWER')")
    @Operation(
        summary = "Inspect a dead-letter webhook event",
        description = "Retrieves full details of a specific dead-letter event by internal UUID, including chronological attempt history and replay audit records. Requires ADMIN, OPERATOR, or VIEWER role."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Dead-letter event details and attempt history",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = DeadLetterDetailResponse.class))
        ),
        @ApiResponse(
            responseCode = "401",
            description = "Unauthorized - authentication credentials missing or invalid",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Forbidden - authenticated user lacks required role",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Event not found with the specified ID",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Event exists but is not in DEAD_LETTER status",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error while inspecting dead-letter event",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<DeadLetterDetailResponse> inspectDeadLetter(
        @Parameter(description = "Internal UUID of the webhook event", required = true)
        @PathVariable("id") UUID id
    ) {
        DeadLetterDetailResponse response = deadLetterService.inspectDeadLetter(id);
        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/{id}/replay", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR')")
    @Operation(
        summary = "Replay a dead-letter webhook event",
        description = "Re-enqueues an existing dead-letter event for asynchronous processing through the transactional outbox. Transitions status to RECEIVED without creating duplicate webhook events. Requires ADMIN or OPERATOR role."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "202",
            description = "Replay request accepted and enqueued for processing",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = WebhookReplayResponsePayload.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid replay request payload",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "401",
            description = "Unauthorized - authentication credentials missing or invalid",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Forbidden - authenticated user lacks required role (VIEWER is denied)",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Event not found with the specified ID",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Event is not in a replayable state (must be in DEAD_LETTER status)",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error during event replay",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<WebhookReplayResponsePayload> replayDeadLetter(
        @Parameter(description = "Internal UUID of the webhook event", required = true)
        @PathVariable("id") UUID id,
        @Valid @RequestBody(required = false) WebhookReplayRequest request
    ) {
        String reason = request != null ? request.reason() : null;
        WebhookReplayResponsePayload response = deadLetterService.replayDeadLetter(id, reason);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
