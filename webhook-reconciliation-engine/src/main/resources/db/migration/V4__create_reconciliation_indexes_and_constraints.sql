-- V4__create_reconciliation_indexes_and_constraints.sql
-- Add uniqueness constraint on webhook_event_id and indexes for reconciliation querying and filtering

ALTER TABLE reconciliation_records
    ADD CONSTRAINT uk_reconciliation_records_event_id UNIQUE (webhook_event_id);

CREATE INDEX idx_reconciliation_records_result_reconciled_at ON reconciliation_records (result, reconciled_at DESC);
CREATE INDEX idx_reconciliation_records_reconciled_at ON reconciliation_records (reconciled_at DESC);
