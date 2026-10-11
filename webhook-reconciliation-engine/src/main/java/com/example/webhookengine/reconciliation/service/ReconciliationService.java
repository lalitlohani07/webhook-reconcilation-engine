package com.example.webhookengine.reconciliation.service;

import com.example.webhookengine.controller.dto.ReconciliationRecordDetailResponse;
import com.example.webhookengine.controller.dto.ReconciliationRecordSummaryResponse;
import com.example.webhookengine.controller.dto.ReconciliationRunRequest;
import com.example.webhookengine.controller.dto.ReconciliationRunResponse;
import com.example.webhookengine.domain.ReconciliationResult;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.UUID;

public interface ReconciliationService {

    ReconciliationRunResponse runReconciliation(ReconciliationRunRequest request);

    Page<ReconciliationRecordSummaryResponse> listRecords(ReconciliationResult result, String source, Pageable pageable);

    ReconciliationRecordDetailResponse getRecord(UUID id);
}
