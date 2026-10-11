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
   - If `attemptCount >= maxAttempts`: updates `WebhookEvent` status to `DEAD_LETTER`, stores `lastError`, and leaves `nextRetryAt` as null.
3. **WebhookRetryScheduler**:
   - Runs on scheduled intervals (`app.retry.scheduler-interval-ms: 1000`).
   - Queries `WebhookEventRepository.findDueEventsForRetry(...)` where `status = 'RETRY_PENDING'` and `nextRetryAt <= now()`, ordered by `nextRetryAt ASC` with pagination.
   - Concurrency & Race Condition Protection: Uses atomic conditional status transition `claimEventForRetry` (`UPDATE status RETRY_PENDING -> RECEIVED`). Only the instance that wins the race proceeds to create an `OutboxEvent` (status: `PENDING`) in the same database transaction.
4. **Separation of Outbox Publication vs. Processing Retries**:
   - Broker publication retries operate strictly at the transport level on `OutboxEvent` (`PENDING`).
   - Business processing retries operate on `WebhookEvent` (`RETRY_PENDING`) and `WebhookAttempt`. A RabbitMQ network outage does not prematurely increment business attempt counts.

## Phase 6 Implementation — Dead-Letter Handling & Controlled Event Replay

### Architectural Overview
Application-level dead-letter handling manages exhausted processing failures without coupling failure state to message broker topology.

```text
Processing Failure (Attempt >= maxAttempts)
   ↓
WebhookProcessingService
   ├── Mark WebhookAttempt as FAILED (completedAt, errorMessage)
   ├── Set WebhookEvent status to DEAD_LETTER
   ├── Clear nextRetryAt (null)
   └── Preserve attemptCount, lastError, and historical attempts
         ↓
WebhookRetryScheduler ignores DEAD_LETTER events
         ↓
Operator Inspection & Replay API (/api/v1/dead-letters)
   ├── GET /api/v1/dead-letters (Paginated summary)
   ├── GET /api/v1/dead-letters/{id} (Detailed failure & attempt history)
   └── POST /api/v1/dead-letters/{id}/replay (Controlled replay via Outbox)
         ↓
[Atomic Database Transaction]
   ├── Claim Event: UPDATE status DEAD_LETTER -> RECEIVED (WHERE status = 'DEAD_LETTER')
   ├── Audit Replay: Insert into webhook_replays (reason, previous_attempt_count, requested_at)
   └── Transactional Outbox: Insert OutboxEvent (PENDING, payload = WebhookProcessingMessage)
         ↓
OutboxPublisher dispatches to RabbitMQ
         ↓
WebhookProcessingConsumer -> WebhookProcessingService executes Attempt N+1
```

### Components & Responsibilities
1. **DeadLetterService**:
   - Coordinates paginated listing of `DEAD_LETTER` events.
   - Provides deep inspection returning `DeadLetterDetailResponse` with chronological attempt history from `webhook_attempts` and audit entries from `webhook_replays`.
   - Executes atomic replay transactions via `claimEventForReplay` and outbox message creation.
2. **DeadLetterController**:
   - Exposes REST endpoints under `/api/v1/dead-letters` with OpenAPI documentation.
   - Returns standard HTTP codes (`200 OK`, `202 Accepted`, `400 Bad Request`, `404 Not Found`, `409 Conflict`).
3. **Database Schema & Audit Migration (V2)**:
   - `idx_webhook_events_status_created_at` on `(status, created_at DESC)` for high-performance paginated dead-letter queries.
   - `webhook_replays` audit table referencing `webhook_events(id)` tracking replay request timestamps, reasons, and previous attempt counts.
4. **Concurrency Safety & Idempotent Replays**:
   - Conditional SQL update `claimEventForReplay` acts as a distributed lock. If two operators or systems trigger replay concurrently, exactly one thread transitions the status and writes the outbox event; the second thread gets 0 updated rows and receives `409 Conflict`.
5. **Attempt Continuity & Bounded Retry Limit**:
   - Replay never resets `attemptCount` to zero or deletes prior `WebhookAttempt` records.
   - The subsequent processing execution consumes attempt number `attemptCount + 1`.
   - If the replayed execution fails, `canRetry(attemptCount + 1)` evaluates to `false` because the attempt count already exceeds `max-attempts`. The event transitions directly back to `DEAD_LETTER`, preventing automated infinite retry storms.

## Security & Role-Based Access Control Architecture (Phase 7)

### 1. Authentication & Operational Security
- **Mechanism**: Spring Security with HTTP Basic authentication (`RestAuthenticationEntryPoint`) and completely stateless session management (`SessionCreationPolicy.STATELESS`).
- **User Persistence**: Operational user accounts are stored in PostgreSQL table `security_users` (managed by Flyway migration `V3__create_security_users_table.sql`).
- **Password Hashing**: Passwords are encrypted using BCrypt (`BCryptPasswordEncoder`). Plaintext passwords are never stored, logged, or returned in API responses.
- **Error Consistency**:
  - `RestAuthenticationEntryPoint` returns HTTP `401 Unauthorized` with `WWW-Authenticate` header and structured JSON `ErrorResponse`.
  - `RestAccessDeniedHandler` returns HTTP `403 Forbidden` with structured JSON `ErrorResponse`.

### 2. Role-Based Authorization Policy
Authorization is enforced through the Spring Security filter chain and method-level security (`@PreAuthorize`):
- `ADMIN`: List dead-letters, inspect dead-letters, replay dead-letters, and manage operational administration.
- `OPERATOR`: List dead-letters, inspect dead-letters, replay dead-letters.
- `VIEWER`: List dead-letters, inspect dead-letters. Forbidden (`403`) from triggering event replays.
- `Public / Unauthenticated`: Public webhook ingestion (`POST /api/v1/webhooks`) and infrastructure health endpoints (`/actuator/health`). All other unlisted application endpoints require authentication by default.

### 3. Bootstrap Administrator Lifecycle
- Controlled via `APP_SECURITY_BOOTSTRAP_USERNAME` and `APP_SECURITY_BOOTSTRAP_PASSWORD`.
- `BootstrapAdminService` implements `ApplicationRunner`:
  - If configuration is absent or blank, bootstrapping is disabled.
  - Checks `securityUserRepository.existsByUsername(username)`: if the admin already exists, bootstrapping skips without overwriting the existing password hash.
  - Plaintext password is never logged.
  - Environment variables are isolated in `.env` (git-ignored) with placeholders in `.env.example`.

### 4. Public Webhook Trust Boundary
- The webhook ingestion endpoint (`POST /api/v1/webhooks`) allows unauthenticated requests from external producers.
- **Trust Boundary Note**: In this phase, the `source` field is client-provided in the payload and can be spoofed (cryptographic webhook signatures are not validated in Phase 7).
- Ingestion is guarded against volumetric overload via Redis rate limiting.

## Redis Rate Limiting Architecture (Phase 7)

### 1. Atomic Fixed-Window Algorithm
- Evaluates traffic per webhook `source` across multiple application instances using a shared Redis instance.
- **Atomic Lua Script**:
  ```lua
  local key = KEYS[1]
  local limit = tonumber(ARGV[1])
  local expireSeconds = tonumber(ARGV[2])

  local current = redis.call('INCR', key)
  if current == 1 then
      redis.call('EXPIRE', key, expireSeconds)
  end
  local ttl = redis.call('TTL', key)
  if ttl == -1 then
      redis.call('EXPIRE', key, expireSeconds)
  end
  return current
  ```
- **Atomicity Guarantee**: Redis executes Lua scripts in a single atomic thread. The `INCR` and conditional `EXPIRE` happen within the same atomic execution, preventing race conditions where keys could be created without TTL expiry.
- **Epoch Partitioning**: Keys are partitioned by clock epoch window (`epochWindow = nowSeconds / windowSeconds`), guaranteeing immediate counter reset when window boundaries turn, and automatic eviction via TTL (`expireSeconds = windowSeconds * 2`).

### 2. Bounded Key Strategy & Normalization
- Redis Key: `app:ratelimit:src:<safeSource>:<epochWindow>`
- Raw `source` values are trimmed and lowercased.
- Safe alphanumeric values (`^[a-z0-9_-]{1,64}$`) are preserved.
- Unbounded strings or values with special characters are converted to SHA-256 hex hashes (`hash_<sha256Hex>`), ensuring Redis keys are bounded and protected against key injection.
- Null or blank values fallback safely to `"unknown"`.

### 3. Rate Limit Behavior & Headers
- **Under Limit**: Ingestion proceeds to transaction and outbox creation. Response contains `X-RateLimit-Limit` and `X-RateLimit-Remaining`.
- **Exceeded Limit**: Request is rejected with HTTP `429 Too Many Requests`, headers `Retry-After: <secondsRemaining>`, `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, and structured `ErrorResponse`.
- **Transactional Safety**: Rejected requests abort before database interaction; no `WebhookEvent` or `OutboxEvent` records are created. Allowed duplicate requests preserve Phase 2 idempotency.

### 4. Redis Outage Handling (Fail-Closed Default)
- **Fail-Closed (Default)**: If Redis is unavailable or times out, the rate limiter throws `RateLimiterUnavailableException`. `GlobalExceptionHandler` converts this to HTTP `503 Service Unavailable` with `Retry-After: 30`. No internal Redis stack traces or keys are leaked to clients.
- **Fail-Open (Configurable)**: If `app.rate-limit.fail-closed=false`, outages log a warning and allow traffic, prioritizing ingestion availability over enforcement.

## Reconciliation Engine Architecture (Phase 8)

### 1. Source-of-Truth Abstraction
- **Interface**: `SourceOfTruthClient` with method `Optional<ExternalEventRecord> fetchExternalRecord(String source, String eventId) throws ExternalProviderException`.
- **Deterministic Simulation (`SimulatedSourceOfTruthClient`)**:
  - Independent of the reconciliation algorithm; provides reference data for local development and automated testing.
  - Suffix patterns:
    - `_notfound`: Simulates missing records in the external provider (`Optional.empty()`).
    - `_mismatch`: Simulates status mismatch (`FAILED` vs `COMPLETED`).
    - `_corrected`: Simulates external settlement with adjustment (`SETTLED_WITH_ADJUSTMENT`).
    - `_error`: Simulates upstream network timeout / outage (`ExternalProviderException`).
    - Default: Simulates matching reference state (`COMPLETED` / `PAID`).
  - Supports programmatic test overrides via `registerRecord(source, eventId, record)` and `registerFailure(source, eventId, errorMessage)`.

### 2. Event Eligibility Rules
Reconciliation operates exclusively on terminal webhook events:
- Internal status must be terminal: `COMPLETED`, `DEAD_LETTER`, or `FAILED`. In-flight events (`RECEIVED`, `PROCESSING`, `RETRY_PENDING`) are excluded from reconciliation until execution concludes.
- By default, events that already possess an audit record in `reconciliation_records` are excluded from new batches.
- Operators may set `includeAlreadyReconciled = true` to force re-evaluation of previously reconciled events.

### 3. Comparison & Classification Rules
Each event is evaluated independently and classified into one of four states:
- `MATCHED`:
  - Internal `COMPLETED` matches external `COMPLETED`, `PAID`, `SUCCEEDED`, `SUCCESS`, or `DELIVERED`.
  - Internal `DEAD_LETTER` / `FAILED` matches external `FAILED`, `DEAD_LETTER`, `CANCELLED`, `REJECTED`, or `ERROR`.
- `CORRECTED`:
  - External status indicates a safe settlement or adjustment (`SETTLED_WITH_ADJUSTMENT`, `RESOLVED_EXTERNALLY`, `MANUALLY_SETTLED`).
  - Safe audit detail is recorded in the JSON audit payload without mutating immutable webhook event history (`WebhookEvent.status` is preserved).
- `MISMATCH`:
  - Internal status contradicts external status (e.g. internal `COMPLETED` but external `FAILED`, or internal `DEAD_LETTER` but external `PAID`).
  - External reference record is not found (`externalStatus = NOT_FOUND`).
- `FAILED`:
  - External provider throws `ExternalProviderException` (network timeout, rate limit, provider 5xx).
  - Recorded as `FAILED` with provider error details; does not abort the remainder of the reconciliation batch.

### 4. Reliability, Deduplication & Concurrency
- **Bounded Batches**: Default batch size is 50, capped at 100 via `ReconciliationProperties`. Prevents unbounded table scans and memory exhaustion.
- **Per-Event Transaction Isolation**: `reconcileSingleEvent` executes with `Propagation.REQUIRES_NEW`. A failure or provider outage on one event commits its `FAILED` audit record without rolling back or failing the remaining batch.
- **Database Uniqueness Constraint**: Flyway `V4__create_reconciliation_indexes_and_constraints.sql` adds `uk_reconciliation_records_event_id` on `reconciliation_records(webhook_event_id)`.
- **Race Condition Tolerance**: If concurrent runs evaluate the same event simultaneously, database constraints prevent duplicate records, and the losing thread catches `DataIntegrityViolationException` to fetch and return the existing record cleanly.

### 5. API & Role-Based Security Matrix
- `POST /api/v1/reconciliation/runs`: Restricted to `ADMIN` and `OPERATOR`. `VIEWER` is denied with `403 Forbidden`.
- `GET /api/v1/reconciliation/records`: Accessible to `ADMIN`, `OPERATOR`, and `VIEWER`. Supports pagination and filtering by `result` and `source`.
- `GET /api/v1/reconciliation/records/{id}`: Accessible to `ADMIN`, `OPERATOR`, and `VIEWER`. Returns detailed record with parsed JSON comparison audit.
- Unauthenticated requests receive HTTP `401 Unauthorized`.

## Current Limitations

- Webhook provider signature verification (HMAC) is not implemented in this phase.
- Automatic mutation of webhook history on `CORRECTED` is intentionally disallowed to preserve audit immutability.
- Administrative web UI is planned for a later phase.

## Future Architecture

The following capabilities are planned:

| Capability | Phase | Notes |
|---|---|---|
| Webhook event persistence | Phase 1 | Completed: Domain model and Flyway migrations |
| Webhook reception endpoints | Phase 2 | Completed: Ingestion API, validation, idempotency |
| RabbitMQ async processing | Phase 3 | Completed: Broker configuration, publisher, consumer, worker lifecycle |
| Transactional Outbox Pattern | Phase 4 | Completed: Atomic DB write, scheduled outbox poller, publisher confirms |
| Retry with exponential backoff | Phase 5 | Completed: Database-backed retries, backoff policy, retry scheduler |
| DLQ & manual replay | Phase 6 | Completed: Dead-letter state, inspection API, outbox replay, audit trail |
| Security & Redis Rate Limiting | Phase 7 | Completed: Basic Auth, RBAC matrix, admin bootstrap, Redis atomic rate limiter |
| Reconciliation engine | Phase 8 | Completed: Source-of-truth abstraction, bounded runs, audit records, RBAC |
| Observability & metrics | Phase 9 | Micrometer Prometheus, dashboards |
| Performance & Production Readiness | Phase 10 | Benchmark, tuning, production checklist |



