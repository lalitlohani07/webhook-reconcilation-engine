-- V2__create_replay_audit_and_indexes.sql
-- Add indexes for dead-letter queries and table for replay audit history

-- 1. Index for dead-letter querying and pagination by creation time
CREATE INDEX idx_webhook_events_status_created_at ON webhook_events (status, created_at DESC);

-- 2. webhook_replays audit table
CREATE TABLE webhook_replays (
    id UUID PRIMARY KEY,
    webhook_event_id UUID NOT NULL,
    requested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    reason VARCHAR(500),
    previous_attempt_count INT NOT NULL,
    CONSTRAINT fk_webhook_replays_event FOREIGN KEY (webhook_event_id)
        REFERENCES webhook_events (id)
);

CREATE INDEX idx_webhook_replays_event_id ON webhook_replays (webhook_event_id);
