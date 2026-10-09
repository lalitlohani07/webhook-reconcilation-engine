package com.example.webhookengine.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(
    name = "webhook_attempts",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_webhook_attempts_event_attempt_number", columnNames = {"webhook_event_id", "attempt_number"})
    }
)
public class WebhookAttempt {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "webhook_event_id", nullable = false)
    private WebhookEvent webhookEvent;

    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 50)
    private WebhookStatus status;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    protected WebhookAttempt() {
        // JPA requirement
    }

    public WebhookAttempt(UUID id, WebhookEvent webhookEvent, int attemptNumber, WebhookStatus status, Instant startedAt) {
        if (webhookEvent == null) {
            throw new IllegalArgumentException("webhookEvent cannot be null");
        }
        if (status == null) {
            throw new IllegalArgumentException("status cannot be null");
        }
        this.id = id != null ? id : UUID.randomUUID();
        this.webhookEvent = webhookEvent;
        this.attemptNumber = attemptNumber;
        this.status = status;
        this.startedAt = startedAt != null ? startedAt : Instant.now();
    }

    public WebhookAttempt(WebhookEvent webhookEvent, int attemptNumber, WebhookStatus status) {
        this(UUID.randomUUID(), webhookEvent, attemptNumber, status, Instant.now());
    }

    @PrePersist
    protected void onCreate() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        if (this.startedAt == null) {
            this.startedAt = Instant.now();
        }
    }

    // Getters and Setters

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public WebhookEvent getWebhookEvent() {
        return webhookEvent;
    }

    public void setWebhookEvent(WebhookEvent webhookEvent) {
        if (webhookEvent == null) {
            throw new IllegalArgumentException("webhookEvent cannot be null");
        }
        this.webhookEvent = webhookEvent;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
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

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WebhookAttempt that)) return false;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
