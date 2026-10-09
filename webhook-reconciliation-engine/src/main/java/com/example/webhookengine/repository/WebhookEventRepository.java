package com.example.webhookengine.repository;

import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WebhookEventRepository extends JpaRepository<WebhookEvent, UUID> {

    Optional<WebhookEvent> findBySourceAndEventId(String source, String eventId);

    List<WebhookEvent> findByStatus(WebhookStatus status);

    List<WebhookEvent> findByStatusAndNextRetryAtLessThanEqual(WebhookStatus status, Instant retryThreshold);

    @org.springframework.data.jpa.repository.Query(
        "SELECT w FROM WebhookEvent w WHERE w.status = :status AND w.nextRetryAt IS NOT NULL AND w.nextRetryAt <= :retryThreshold ORDER BY w.nextRetryAt ASC"
    )
    List<WebhookEvent> findDueEventsForRetry(
        @org.springframework.data.repository.query.Param("status") WebhookStatus status,
        @org.springframework.data.repository.query.Param("retryThreshold") Instant retryThreshold,
        org.springframework.data.domain.Pageable pageable
    );

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @org.springframework.data.jpa.repository.Query(
        "UPDATE WebhookEvent w SET w.status = :newStatus, w.updatedAt = :now WHERE w.id = :id AND w.status = :currentStatus"
    )
    int claimEventForRetry(
        @org.springframework.data.repository.query.Param("id") UUID id,
        @org.springframework.data.repository.query.Param("currentStatus") WebhookStatus currentStatus,
        @org.springframework.data.repository.query.Param("newStatus") WebhookStatus newStatus,
        @org.springframework.data.repository.query.Param("now") Instant now
    );
}
