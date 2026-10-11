package com.example.webhookengine.repository;

import com.example.webhookengine.domain.ReconciliationRecord;
import com.example.webhookengine.domain.WebhookEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ReconciliationRecordRepository extends JpaRepository<ReconciliationRecord, UUID> {

    List<ReconciliationRecord> findByWebhookEventOrderByReconciledAtDesc(WebhookEvent webhookEvent);

    Optional<ReconciliationRecord> findByWebhookEvent(WebhookEvent webhookEvent);

    Optional<ReconciliationRecord> findByWebhookEventId(UUID webhookEventId);

    boolean existsByWebhookEventId(UUID webhookEventId);

    @org.springframework.data.jpa.repository.EntityGraph(attributePaths = {"webhookEvent"})
    Optional<ReconciliationRecord> findById(UUID id);

    @org.springframework.data.jpa.repository.EntityGraph(attributePaths = {"webhookEvent"})
    Page<ReconciliationRecord> findAll(Pageable pageable);

    @org.springframework.data.jpa.repository.EntityGraph(attributePaths = {"webhookEvent"})
    Page<ReconciliationRecord> findByResult(com.example.webhookengine.domain.ReconciliationResult result, Pageable pageable);

    @org.springframework.data.jpa.repository.EntityGraph(attributePaths = {"webhookEvent"})
    Page<ReconciliationRecord> findByWebhookEventSource(String source, Pageable pageable);

    @org.springframework.data.jpa.repository.EntityGraph(attributePaths = {"webhookEvent"})
    Page<ReconciliationRecord> findByResultAndWebhookEventSource(com.example.webhookengine.domain.ReconciliationResult result, String source, Pageable pageable);
}
