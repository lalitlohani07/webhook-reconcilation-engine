package com.example.webhookengine.repository;

import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.WebhookEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ReconciliationRecordRepository extends JpaRepository<ReconciliationRecord, UUID> {

    List<ReconciliationRecord> findByWebhookEventOrderByReconciledAtDesc(WebhookEvent webhookEvent);
}
