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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Objects;
import jakarta.persistence.UniqueConstraint;
import java.util.UUID;

@Entity
@Table(
    name = "reconciliation_records",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_reconciliation_records_event_id", columnNames = {"webhook_event_id"})
    }
)
public class ReconciliationRecord {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "webhook_event_id", nullable = false)
    private WebhookEvent webhookEvent;

    @Column(name = "external_status", length = 50)
    private String externalStatus;

    @Column(name = "internal_status", length = 50)
    private String internalStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "result", nullable = false, length = 50)
    private ReconciliationResult result;

    @Column(name = "reconciled_at", nullable = false)
    private Instant reconciledAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "details", columnDefinition = "jsonb")
    private String details;

    protected ReconciliationRecord() {
        // JPA requirement
    }

    public ReconciliationRecord(UUID id, WebhookEvent webhookEvent, String externalStatus, String internalStatus,
                                ReconciliationResult result, Instant reconciledAt, String details) {
        if (webhookEvent == null) {
            throw new IllegalArgumentException("webhookEvent cannot be null");
        }
        if (result == null) {
            throw new IllegalArgumentException("result cannot be null");
        }
        this.id = id != null ? id : UUID.randomUUID();
        this.webhookEvent = webhookEvent;
        this.externalStatus = externalStatus;
        this.internalStatus = internalStatus;
        this.result = result;
        this.reconciledAt = reconciledAt != null ? reconciledAt : Instant.now();
        this.details = details;
    }

    public ReconciliationRecord(WebhookEvent webhookEvent, String externalStatus, String internalStatus,
                                ReconciliationResult result, String details) {
        this(UUID.randomUUID(), webhookEvent, externalStatus, internalStatus, result, Instant.now(), details);
    }

    @PrePersist
    protected void onCreate() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
        if (this.reconciledAt == null) {
            this.reconciledAt = Instant.now();
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

    public String getExternalStatus() {
        return externalStatus;
    }

    public void setExternalStatus(String externalStatus) {
        this.externalStatus = externalStatus;
    }

    public String getInternalStatus() {
        return internalStatus;
    }

    public void setInternalStatus(String internalStatus) {
        this.internalStatus = internalStatus;
    }

    public ReconciliationResult getResult() {
        return result;
    }

    public void setResult(ReconciliationResult result) {
        if (result == null) {
            throw new IllegalArgumentException("result cannot be null");
        }
        this.result = result;
    }

    public Instant getReconciledAt() {
        return reconciledAt;
    }

    public void setReconciledAt(Instant reconciledAt) {
        this.reconciledAt = reconciledAt;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReconciliationRecord that)) return false;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
