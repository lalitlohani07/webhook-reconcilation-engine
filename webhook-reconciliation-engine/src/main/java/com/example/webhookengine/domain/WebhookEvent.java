package com.example.webhookengine.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(
    name = "webhook_events",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_webhook_events_source_event_id", columnNames = {"source", "event_id"})
    }
)
public class WebhookEvent {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false, length = 255)
    private String eventId;

    @Column(name = "source", nullable = false, length = 100)
    private String source;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    private WebhookStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount = 0;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(name = "next_retry_at")
    private Instant nextRetryAt;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    protected WebhookEvent() {
        // JPA requirement
    }

    public WebhookEvent(UUID id, String eventId, String source, String eventType, String payload, WebhookStatus status) {
        validateRequiredFields(eventId, source, eventType, payload, status);
        this.id = id != null ? id : UUID.randomUUID();
        this.eventId = eventId;
        this.source = source;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.attemptCount = 0;
    }

    public WebhookEvent(String eventId, String source, String eventType, String payload, WebhookStatus status) {
        this(UUID.randomUUID(), eventId, source, eventType, payload, status);
    }

    private void validateRequiredFields(String eventId, String source, String eventType, String payload, WebhookStatus status) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId cannot be null or blank");
        }
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source cannot be null or blank");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType cannot be null or blank");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload cannot be null");
        }
        if (status == null) {
            throw new IllegalArgumentException("status cannot be null");
        }
    }

    @PrePersist
    protected void onCreate() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        Instant now = Instant.now();
        if (this.createdAt == null) {
            this.createdAt = now;
        }
        if (this.updatedAt == null) {
            this.updatedAt = now;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }

    // Getters and Setters

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId cannot be null or blank");
        }
        this.eventId = eventId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source cannot be null or blank");
        }
        this.source = source;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType cannot be null or blank");
        }
        this.eventType = eventType;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload cannot be null");
        }
        this.payload = payload;
    }

    public WebhookStatus getStatus() {
        return status;
    }

    public void setStatus(WebhookStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("status cannot be null");
        }
        this.status = status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    public Instant getNextRetryAt() {
        return nextRetryAt;
    }

    public void setNextRetryAt(Instant nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WebhookEvent that)) return false;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
