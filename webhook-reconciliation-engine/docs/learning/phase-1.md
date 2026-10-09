# Phase 1 Learning Checklist

- [x] **JPA** — Java Persistence API specification using Jakarta Persistence annotations (`@Entity`, `@Table`, `@Id`, `@Column`, `@ManyToOne`, `@Enumerated`, `@PrePersist`, `@PreUpdate`).
- [x] **Hibernate** — ORM provider configured with `ddl-auto: validate` so that Hibernate enforces entity-to-schema parity without touching schema DDL.
- [x] **Entity mapping** — Clean POJO entity modeling with required field validation, immutability where appropriate (`updatable = false` on primary keys and creation timestamps).
- [x] **Relationships** — Unidirectional `@ManyToOne(fetch = FetchType.LAZY, optional = false)` mapping on `WebhookAttempt` and `ReconciliationRecord` referencing `WebhookEvent`, without cascading deletes to prevent accidental audit loss.
- [x] **UUID** — Client/application generated UUIDs (`UUID.randomUUID()`) as primary keys for distributed identification.
- [x] **Unique constraints** — Database-level uniqueness on composite keys: `(source, event_id)` for webhook events and `(webhook_event_id, attempt_number)` for attempts.
- [x] **Indexes** — Targeted B-Tree indexes on query predicates (`(source, event_id)`, `status`, `next_retry_at`, `created_at`, `webhook_event_id`).
- [x] **JSONB** — Storing payload and details as PostgreSQL `jsonb` using Hibernate 6's `@JdbcTypeCode(SqlTypes.JSON)` for arbitrary JSON payloads without custom serialization overhead.
- [x] **Flyway** — Version-controlled migration scripts (`V1__create_webhook_domain_tables.sql`) as the single source of truth for the database schema.
- [x] **Transactions** — Spring Data JPA repositories with standard declarative transaction boundaries.
- [x] **Idempotency** — Idempotent ingestion foundation via database-enforced `UNIQUE(source, event_id)` constraint allowing external event IDs to collide across providers while preventing duplicate processing.
- [x] **Outbox Pattern** — `OutboxEvent` table ready for transactional event publication to RabbitMQ in Phase 4.
