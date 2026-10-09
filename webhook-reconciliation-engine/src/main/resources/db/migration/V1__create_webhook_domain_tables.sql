-- V1__create_webhook_domain_tables.sql
-- Minimum domain model tables for Webhook Reconciliation Engine MVP

-- 1. webhook_events
CREATE TABLE webhook_events (
    id UUID PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    source VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    CONSTRAINT uk_webhook_events_source_event_id UNIQUE (source, event_id)
);

CREATE INDEX idx_webhook_events_source_event_id ON webhook_events (source, event_id);
CREATE INDEX idx_webhook_events_status ON webhook_events (status);
CREATE INDEX idx_webhook_events_next_retry_at ON webhook_events (next_retry_at);
CREATE INDEX idx_webhook_events_created_at ON webhook_events (created_at);

-- 2. webhook_attempts
CREATE TABLE webhook_attempts (
    id UUID PRIMARY KEY,
    webhook_event_id UUID NOT NULL,
    attempt_number INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    CONSTRAINT fk_webhook_attempts_event FOREIGN KEY (webhook_event_id)
        REFERENCES webhook_events (id),
    CONSTRAINT uk_webhook_attempts_event_attempt_number UNIQUE (webhook_event_id, attempt_number)
);

CREATE INDEX idx_webhook_attempts_event_id ON webhook_attempts (webhook_event_id);

-- 3. outbox_events
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX idx_outbox_events_status ON outbox_events (status);

-- 4. reconciliation_records
CREATE TABLE reconciliation_records (
    id UUID PRIMARY KEY,
    webhook_event_id UUID NOT NULL,
    external_status VARCHAR(50),
    internal_status VARCHAR(50),
    result VARCHAR(50) NOT NULL,
    reconciled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    details JSONB,
    CONSTRAINT fk_reconciliation_records_event FOREIGN KEY (webhook_event_id)
        REFERENCES webhook_events (id)
);

CREATE INDEX idx_reconciliation_records_event_id ON reconciliation_records (webhook_event_id);
