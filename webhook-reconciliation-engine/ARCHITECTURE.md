# Architecture — Webhook Reconciliation Engine

## Project Purpose

The Webhook Reconciliation Engine is a backend platform that reliably receives, validates, persists, and processes webhook events from external services. It reconciles internal state against external sources of truth and provides full observability into the processing pipeline.

## Architectural Style

The project follows a **layered architecture** with clear separation of concerns:

- **Controller Layer** — Handles HTTP requests and responses
- **Service Layer** — Contains business logic
- **Repository Layer** — Manages data persistence
- **Messaging Layer** — Handles asynchronous message processing via RabbitMQ
- **Domain Layer** — Defines core business entities

This architecture was chosen for its simplicity and because the system's complexity does not yet warrant event sourcing, CQRS, or hexagonal architecture. The layered approach can be refactored incrementally if needed.

## Package Structure

```
com.example.webhookengine/
├── controller/         # REST API endpoints
├── service/            # Business logic and orchestration
├── repository/         # Data access (Spring Data JDBC)
├── domain/             # Domain entities and value objects
├── messaging/          # RabbitMQ producers and consumers
├── reconciliation/     # Reconciliation logic
├── security/           # Webhook authentication, signature validation
├── exception/          # Custom exceptions, global error handlers
├── config/             # Spring configuration classes
└── common/             # Shared utilities and constants
```

## Infrastructure Components

### PostgreSQL — Primary Database

**Status: CURRENTLY IMPLEMENTED (connection only)**

PostgreSQL is the primary relational database. It was chosen because:

- ACID compliance is critical for webhook event persistence — we cannot lose or duplicate events
- Strong support for JSON/JSONB columns for storing arbitrary webhook payloads
- Mature ecosystem with excellent JDBC driver and Spring Data support
- Flyway provides reliable, version-controlled schema migrations
- Well-suited for the transactional consistency requirements of reconciliation

Currently, the database connection is configured and Flyway is enabled, but no application tables have been created.

### RabbitMQ — Message Broker

**Status: CURRENTLY IMPLEMENTED (connection only)**

RabbitMQ handles asynchronous event processing. It was chosen because:

- Native support for acknowledgments, dead letter exchanges, and message TTL
- Built-in retry semantics with dead letter queues
- Supports multiple exchange types (direct, topic, fanout) for flexible routing
- Management UI for operational visibility
- Spring AMQP provides mature integration

Currently, the RabbitMQ connection is configured but no queues, exchanges, or consumers are defined.

### Redis — Cache and Distributed State

**Status: CURRENTLY IMPLEMENTED (connection only)**

Redis provides caching and distributed state management. It was chosen because:

- Sub-millisecond lookups for idempotency key checking
- Atomic operations (SET NX, EXPIRE) for distributed locking
- TTL-based key expiration aligns well with idempotency windows
- Simple key-value model fits the caching use cases

Currently, the Redis connection is configured but no application-level caching or state management is implemented.

## Asynchronous Processing

**Status: PLANNED**

Webhook events will be processed asynchronously to:

- Decouple webhook reception from processing, ensuring fast HTTP responses
- Enable independent scaling of reception and processing
- Provide natural retry semantics via message redelivery
- Prevent processing failures from blocking new webhook reception

## Current Implementation (Phase 0)

What is currently in place:

- [x] Spring Boot application skeleton
- [x] PostgreSQL connection with HikariCP connection pool
- [x] RabbitMQ connection via Spring AMQP
- [x] Redis connection via Spring Data Redis
- [x] Flyway configured (no migrations yet)
- [x] Spring Actuator with health checks for all infrastructure
- [x] OpenAPI/Swagger documentation infrastructure
- [x] Graceful shutdown configuration
- [x] Docker Compose for local infrastructure
- [x] Multi-stage Dockerfile for application containerization
- [x] Testcontainers integration for testing
- [x] Structured logging configuration

## Phase 1 Implementation — Domain Model & Database Schema

### Entities & Relationships
1. **WebhookEvent**:
   - Primary domain entity representing received incoming webhooks.
   - Identifier: UUID `id` (primary key). External `eventId` is kept separate.
   - Composite unique constraint: `UNIQUE(source, event_id)`. Different providers (e.g., Stripe and GitHub) may produce identical event IDs (`evt_123`), which is safely allowed, while preventing duplicates from the same source.
   - Status: `WebhookStatus` enum (`RECEIVED`, `PROCESSING`, `COMPLETED`, `RETRY_PENDING`, `FAILED`, `DEAD_LETTER`), persisted as `VARCHAR(50)` strings (avoiding native PostgreSQL ENUM fragility).
   - Payload: JSON/JSONB-compatible storage using Hibernate `@JdbcTypeCode(SqlTypes.JSON)`.
   - Timestamps: `Instant` stored as `TIMESTAMP WITH TIME ZONE` (UTC).

2. **WebhookAttempt**:
   - Tracks individual delivery and processing execution attempts for an event (`1:N` from `WebhookEvent`).
   - Unique constraint: `UNIQUE(webhook_event_id, attempt_number)`.
   - Preserves attempt start/completion timestamps and error messages without duplicating payload data.
   - Foreign key to `webhook_events(id)` without cascading deletes to maintain historical auditability.

3. **OutboxEvent**:
   - Represents transactional outbox records for reliable event publication to RabbitMQ.
   - Fields: `id` (UUID), `aggregateId`, `eventType`, `payload` (JSONB), `status` (`PENDING`, `PUBLISHED`, `FAILED`), timestamps, retry count, and last error.
   - Prepared for the **Transactional Outbox Pattern**: In Phase 4, domain state mutations and outgoing broker events are committed in the same database transaction, eliminating dual-write failure modes between the database and RabbitMQ.

4. **ReconciliationRecord**:
   - Captures verification and reconciliation results comparing internal state with the external source of truth.
   - Fields: `id` (UUID), `webhookEvent` (foreign key), `externalStatus`, `internalStatus`, `result` (`MATCHED`, `CORRECTED`, `MISMATCH`, `FAILED`), `reconciledAt`, and `details` (JSONB).

### Database Responsibility & Schema Ownership
- **Flyway owns schema creation**: Schema migrations are strictly managed through versioned SQL scripts in `src/main/resources/db/migration/` (`V1__create_webhook_domain_tables.sql`).
- **JPA/Hibernate validation**: Configured with `spring.jpa.hibernate.ddl-auto: validate`. Hibernate validates entity mappings against the database schema at application startup without creating, altering, or dropping tables in production.
- **Why JPA/Hibernate is used**: Provides standard object-relational mapping, lifecycle callbacks (`@PrePersist`, `@PreUpdate`), transactional repository support, and clean relationship navigation between events, attempts, and reconciliation records.

### Timestamps & JSONB
- **Timestamps**: All timestamps use Java `Instant` mapped to PostgreSQL `TIMESTAMP WITH TIME ZONE` to guarantee timezone-independent UTC persistence.
- **JSONB**: Payloads and details are mapped as string-based JSONB via `@JdbcTypeCode(SqlTypes.JSON)` and `columnDefinition = "jsonb"`, providing native PostgreSQL JSON indexing and query capabilities without complex custom serializers.

## Phase 2 Implementation — Webhook Ingestion API & Idempotency

### Layered Flow
```text
HTTP Client
   ↓ POST /api/v1/webhooks
WebhookController (Validation, DTO mapping)
   ↓ WebhookIngestRequest
WebhookIngestionService (Idempotency logic, concurrency fallback)
   ↓ findBySourceAndEventId / saveAndFlush
WebhookEventRepository (PostgreSQL / Flyway Schema)
```

### Idempotency and Concurrency Strategy
- **Sequential Ingestion**: Checks `findBySourceAndEventId(source, eventId)`. If present, returns `200 OK` with existing status. If absent, persists the event with status `RECEIVED` and returns `202 Accepted`.
- **Concurrent Ingestion**: When two concurrent threads simultaneously evaluate `findBySourceAndEventId` and neither sees an existing record, both attempt `saveAndFlush`. The database uniqueness constraint `UNIQUE(source, event_id)` rejects the duplicate with `DataIntegrityViolationException`. The service catches this exception and retrieves the safely persisted record, guaranteeing that exactly one record exists.
- **Payload & State Preservation**: Duplicates never overwrite existing payloads, statuses, attempt counts, or timestamps.

## Phase 3 Implementation — RabbitMQ & Asynchronous Worker

### Processing Pipeline
```text
HTTP Client
   ↓ POST /api/v1/webhooks
WebhookController (Validation)
   ↓
WebhookIngestionService (Idempotent persistence in PostgreSQL)
   ↓ saveAndFlush (status: RECEIVED)
WebhookEventPublisher
   ↓ convertAndSend (exchange: webhook.events, routingKey: webhook.received)
RabbitMQ Queue: webhook.processing
   ↓ @RabbitListener
WebhookProcessingConsumer
   ↓ processEventById(internalUUID)
WebhookProcessingService (Authoritative DB check & state claim)
   ↓ load from PostgreSQL
   ├── Check status: if COMPLETED or PROCESSING -> skip duplicate
   ├── Update status: PROCESSING, attemptCount + 1
   ├── Persist WebhookAttempt (status: PROCESSING)
   ├── WebhookEventProcessor.processEvent(event)
   └── Update status: COMPLETED, processedAt = now(), WebhookAttempt = COMPLETED
```

### Components & Responsibilities
1. **RabbitMqConfig**: Declares durable `webhook.events` DirectExchange, durable `webhook.processing` Queue, binding with routing key `webhook.received`, JSON message converter, publisher confirms, and returns callbacks.
2. **WebhookEventPublisher**: Constructs lightweight `WebhookProcessingMessage` (containing only internal `webhookEventId`, external `eventId`, `source`, `eventType`) and publishes to RabbitMQ upon new event acceptance.
3. **WebhookProcessingConsumer**: Listens to `webhook.processing`, unmarshals `WebhookProcessingMessage`, and delegates to service layer.
4. **WebhookProcessingService**: Orchestrates database claim, attempt tracking, status transitions (`RECEIVED → PROCESSING → COMPLETED`), and handles failures (`FAILED`).
5. **WebhookEventProcessor**: Deterministic business logic component separated from messaging listeners.

### Duplicate Message Delivery & Concurrency Safety
- **Authoritative Database State**: PostgreSQL remains the single source of truth.
- **Idempotent Consumption**: When duplicate messages arrive, the consumer inspects `WebhookEvent.getStatus()`. If `COMPLETED`, it acknowledges the message without reprocessing. If `PROCESSING`, concurrent execution is avoided.
## Phase 4 Implementation — Transactional Outbox Pattern
 
### Transactional Ingestion & Outbox Persistence
To eliminate the dual-write race condition between PostgreSQL and RabbitMQ, incoming webhooks persist both the domain record and the outbox event atomically:
```text
HTTP Client
   ↓ POST /api/v1/webhooks
WebhookController
   ↓
WebhookIngestionService.ingestWebhook
   ↓
persistEventAndOutbox [SINGLE @Transactional POSTGRESQL BOUNDARY]
   ├── webhookEventRepository.saveAndFlush(WebhookEvent: RECEIVED)
   └── outboxEventRepository.saveAndFlush(OutboxEvent: PENDING)
   ↓
Return 202 Accepted (No direct broker publishing during HTTP ingestion)
```

### Outbox Publisher Poller
1. **Scheduled Polling**: `OutboxPublisher` runs at configurable intervals (`app.outbox.poll-interval-ms: 1000`) with batching (`app.outbox.batch-size: 10`).
2. **Deterministic FIFO Ordering**: `findPendingEventsForProcessing` queries `status = 'PENDING'` ordered by `createdAt ASC` using `Pageable`.
3. **Publisher Confirmation & Returns**:
   - `WebhookEventPublisher` utilizes RabbitMQ correlation futures (`correlationData.getFuture().get(5, SECONDS)`).
   - If broker returns ACK without return warning, `outboxEvent.markPublished()` is committed (`status = PUBLISHED`, `publishedAt = Instant.now()`).
   - If broker returns NACK, unroutable message, or times out, the outbox record remains `PENDING` with `retryCount` incremented and `lastError` recorded for automatic redelivery on the next poller cycle.
4. **Resilience & Fault Tolerance**:
   - **PostgreSQL Failure**: Neither `WebhookEvent` nor `OutboxEvent` is committed (tested via transaction rollback).
   - **RabbitMQ Downtime**: Ingestion continues uninterrupted with `202 Accepted`. Outbox records accumulate as `PENDING` and are published seamlessly once RabbitMQ recovers.
   - **Application Crash**: Unsent `PENDING` events are picked up by the publisher upon restart.
   - **At-Least-Once Delivery**: Downstream workers continue to rely on database state checks (`status != RECEIVED`) to deduplicate redelivered messages.

## Phase 5 Implementation — Retry Handling & Exponential Backoff

### Architectural Flow & Components
1. **RetryPolicy**:
   - Implements exponential backoff: $d_n = \min(d_0 \times m^{n-1}, d_{\max})$.
   - Calculates duration and deterministic UTC `nextRetryAt` timestamps.
   - Evaluates `canRetry(attemptCount)` using `attemptCount < maxAttempts`.
2. **Failure Handling Workflow in WebhookProcessingService**:
   - On processor failure, creates and persists a failed `WebhookAttempt` with error details, attempt number, and completion time.
   - If `attemptCount < maxAttempts`: updates `WebhookEvent` status to `RETRY_PENDING`, stores `lastError`, and persists `nextRetryAt`.
   - If `attemptCount >= maxAttempts`: updates `WebhookEvent` status to `FAILED`, stores `lastError`, and leaves `nextRetryAt` as null.
3. **WebhookRetryScheduler**:
   - Runs on scheduled intervals (`app.retry.scheduler-interval-ms: 1000`).
   - Queries `WebhookEventRepository.findDueEventsForRetry(...)` where `status = 'RETRY_PENDING'` and `nextRetryAt <= now()`, ordered by `nextRetryAt ASC` with pagination.
   - Concurrency & Race Condition Protection: Uses atomic conditional status transition `claimEventForRetry` (`UPDATE status RETRY_PENDING -> RECEIVED`). Only the instance that wins the race proceeds to create an `OutboxEvent` (status: `PENDING`) in the same database transaction.
4. **Separation of Outbox Publication vs. Processing Retries**:
   - Broker publication retries operate strictly at the transport level on `OutboxEvent` (`PENDING`).
   - Business processing retries operate on `WebhookEvent` (`RETRY_PENDING`) and `WebhookAttempt`. A RabbitMQ network outage does not prematurely increment business attempt counts.

## Current Limitations

- No Dead Letter Queue or replay endpoints (Phase 6)
- No reconciliation logic against external APIs (Phase 6)
- No Redis rate limiting or caching (Phase 3 in old numbering / Redis layer)
- No webhook signature validation or security authentication (Phase 9)
- No administrative UI (Phase 8)

## Future Architecture

The following capabilities are **planned** but not implemented:

| Capability | Phase | Notes |
|---|---|---|
| Webhook event persistence | Phase 1 | Completed: Domain model and Flyway migrations |
| Webhook reception endpoints | Phase 2 | Completed: Ingestion API, validation, idempotency |
| RabbitMQ async processing | Phase 3 | Completed: Broker configuration, publisher, consumer, worker lifecycle |
| Transactional Outbox Pattern | Phase 4 | Completed: Atomic DB write, scheduled outbox poller, publisher confirms |
| Retry with exponential backoff | Phase 5 | Completed: Database-backed retries, backoff policy, retry scheduler |
| DLQ & manual replay | Phase 6 | Dead-letter queue for exhausted events, manual replay endpoint |
| Reconciliation engine | Phase 7 | External source-of-truth comparison |
| Observability & metrics | Phase 8 | Micrometer, dashboards |
| Admin dashboard | Phase 9 | Event browser, replay, monitoring |
| Security hardening | Phase 10 | Rate limiting, API keys, HMAC |


