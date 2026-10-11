package com.example.webhookengine.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "webhook_replays")
public class WebhookReplay {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "webhook_event_id", nullable = false)
    private WebhookEvent webhookEvent;

    @Column(name = "requested_at", nullable = false)
    private Instant requestedAt;

    @Column(name = "reason", length = 500)
    private String reason;

    @Column(name = "previous_attempt_count", nullable = false)
    private int previousAttemptCount;

    protected WebhookReplay() {
        // JPA requirement
    }

    public WebhookReplay(UUID id, WebhookEvent webhookEvent, Instant requestedAt, String reason, int previousAttemptCount) {
        if (webhookEvent == null) {
            throw new IllegalArgumentException("webhookEvent cannot be null");
        }
        this.id = id != null ? id : UUID.randomUUID();
        this.webhookEvent = webhookEvent;
        this.requestedAt = requestedAt != null ? requestedAt : Instant.now();
        this.reason = reason;
        this.previousAttemptCount = previousAttemptCount;
    }

    public WebhookReplay(WebhookEvent webhookEvent, String reason, int previousAttemptCount) {
        this(UUID.randomUUID(), webhookEvent, Instant.now(), reason, previousAttemptCount);
    }

    @PrePersist
    protected void onCreate() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        if (this.requestedAt == null) {
            this.requestedAt = Instant.now();
        }
    }

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

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public void setRequestedAt(Instant requestedAt) {
        this.requestedAt = requestedAt;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public int getPreviousAttemptCount() {
        return previousAttemptCount;
    }

    public void setPreviousAttemptCount(int previousAttemptCount) {
        this.previousAttemptCount = previousAttemptCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WebhookReplay that)) return false;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
