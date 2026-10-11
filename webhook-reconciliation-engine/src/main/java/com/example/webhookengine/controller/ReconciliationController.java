package com.example.webhookengine.controller;

import com.example.webhookengine.controller.dto.ErrorResponse;
import com.example.webhookengine.controller.dto.ReconciliationRecordDetailResponse;
import com.example.webhookengine.controller.dto.ReconciliationRecordSummaryResponse;
import com.example.webhookengine.controller.dto.ReconciliationRunRequest;
import com.example.webhookengine.controller.dto.ReconciliationRunResponse;
import com.example.webhookengine.domain.ReconciliationResult;
import com.example.webhookengine.reconciliation.service.ReconciliationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/reconciliation")
@Tag(name = "Reconciliation", description = "Endpoints for initiating reconciliation runs and inspecting reconciliation records")
@SecurityRequirement(name = "basicAuth")
public class ReconciliationController {

    private final ReconciliationService reconciliationService;

    public ReconciliationController(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @PostMapping(value = "/runs", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR')")
    @Operation(
        summary = "Initiate bounded reconciliation run",
        description = "Executes a bounded batch reconciliation workflow comparing local webhook events against the external source of truth. Requires ADMIN or OPERATOR role."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Reconciliation run executed successfully with batch statistics",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ReconciliationRunResponse.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid batch request parameters (e.g. batch size out of bounds)",
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
            responseCode = "500",
            description = "Internal server error during reconciliation batch execution",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<ReconciliationRunResponse> runReconciliation(
        @Valid @RequestBody(required = false) ReconciliationRunRequest request
    ) {
        ReconciliationRunRequest effectiveRequest = request != null ? request : new ReconciliationRunRequest();
        ReconciliationRunResponse response = reconciliationService.runReconciliation(effectiveRequest);
        return ResponseEntity.ok(response);
    }

    @GetMapping(value = "/records", produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR', 'VIEWER')")
    @Operation(
        summary = "List reconciliation records",
        description = "Retrieves a paginated list of reconciliation audit records with optional filtering by result and source. Requires ADMIN, OPERATOR, or VIEWER role."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Paginated list of reconciliation records",
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
            description = "Internal server error while retrieving reconciliation records",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<Page<ReconciliationRecordSummaryResponse>> listRecords(
        @Parameter(description = "Optional filter by reconciliation result (MATCHED, CORRECTED, MISMATCH, FAILED)")
        @RequestParam(name = "result", required = false) ReconciliationResult result,
        @Parameter(description = "Optional filter by webhook event source")
        @RequestParam(name = "source", required = false) String source,
        @PageableDefault(size = 20, sort = "reconciledAt", direction = Sort.Direction.DESC) Pageable pageable
    ) {
        Page<ReconciliationRecordSummaryResponse> page = reconciliationService.listRecords(result, source, pageable);
        return ResponseEntity.ok(page);
    }

    @GetMapping(value = "/records/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR', 'VIEWER')")
    @Operation(
        summary = "Inspect a reconciliation record",
        description = "Retrieves complete details of a specific reconciliation record including comparison audit JSON. Requires ADMIN, OPERATOR, or VIEWER role."
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Reconciliation record detail found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ReconciliationRecordDetailResponse.class))
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
            description = "Reconciliation record not found with the specified ID",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error while inspecting reconciliation record",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class))
        )
    })
    public ResponseEntity<ReconciliationRecordDetailResponse> getRecord(
        @Parameter(description = "Internal UUID of the reconciliation record", required = true)
        @PathVariable("id") UUID id
    ) {
        ReconciliationRecordDetailResponse response = reconciliationService.getRecord(id);
        return ResponseEntity.ok(response);
    }
}
