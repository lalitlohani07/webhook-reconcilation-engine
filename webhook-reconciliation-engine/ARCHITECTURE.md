# Technical Architecture — Webhook Reconciliation Engine

## 1. System Purpose & Architectural Style

The **Webhook Reconciliation Engine** is an event-driven Java backend platform engineered to reliably receive, persist, process, and reconcile webhook events emitted by external distributed systems.

### Layered Architecture
The application adheres to a clean **layered architecture** with unidirectional dependencies:

```
┌─────────────────────────────────────────────────────────────┐
│                       Controller Layer                      │
│   (REST Controllers, DTO Validation, Rate Limiter Check)    │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                        Service Layer                        │
│   (Business Orchestration, Retry Engine, DLQ, Reconciliation│
└──────────────┬──────────────────────────────┬───────────────┘
               │                              │
┌──────────────▼──────────────┐┌──────────────▼───────────────┐
│       Messaging Layer       ││       Repository Layer       │
│(RabbitMQ Publishers/Consumer││ (Spring Data JPA, Row Locks) │
└──────────────┬──────────────┘└──────────────┬───────────────┘
               │                              │
┌──────────────▼──────────────┐┌──────────────▼───────────────┐
│     RabbitMQ Message Broker ││     PostgreSQL 16 Database   │
└─────────────────────────────┘└──────────────────────────────┘
```

1. **Controller Layer**: Handles HTTP ingress, validates request DTOs (`@Valid`), executes pre-flight Redis rate limit checks, and translates domain exceptions into standard HTTP error responses.
2. **Service Layer**: Orchestrates domain business logic, state transitions, transaction management, backoff calculations, replay claims, and reconciliation comparisons.
3. **Repository Layer**: Encapsulates persistence operations against PostgreSQL using Spring Data JPA, enforcing explicit concurrency controls via pessimistic row locks (`@Lock(LockModeType.PESSIMISTIC_WRITE)`).
4. **Messaging Layer**: Implements AMQP publishing and consumption with RabbitMQ using correlated publisher confirms and JSON serialization.
5. **Domain Layer**: Encapsulates core business entities, status enums, and JPA mappings.

---

## 2. Domain Model & Entity Relationships

The data model is partitioned into distinct audit trails for domain events, worker attempts, outbox publications, operator replays, reconciliation records, and operational accounts.

```mermaid
erDiagram
    WEBHOOK_EVENT ||--o{ WEBHOOK_ATTEMPT : "has attempts"
    WEBHOOK_EVENT ||--o{ OUTBOX_EVENT : "emits"
    WEBHOOK_EVENT ||--o{ WEBHOOK_REPLAY : "replayed via"
    WEBHOOK_EVENT ||--o| RECONCILIATION_RECORD : "reconciled in"

    WEBHOOK_EVENT {
        uuid id PK
        varchar source
        varchar event_id
        varchar event_type
        varchar status
        int attempt_count
        timestamp next_retry_at
        text last_error
        jsonb payload
        timestamp created_at
        timestamp updated_at
        timestamp processed_at
    }

    WEBHOOK_ATTEMPT {
        uuid id PK
        uuid webhook_event_id FK
        int attempt_number
        varchar status
        text error_message
        timestamp started_at
        timestamp completed_at
    }

    OUTBOX_EVENT {
        uuid id PK
        uuid aggregate_id
        varchar event_type
        varchar status
        jsonb payload
        int retry_count
        text last_error
        timestamp created_at
        timestamp published_at
    }

    WEBHOOK_REPLAY {
        uuid id PK
        uuid webhook_event_id FK
        varchar reason
        int previous_attempt_count
        timestamp requested_at
    }

    RECONCILIATION_RECORD {
        uuid id PK
        uuid webhook_event_id FK
        varchar event_id
        varchar source
        varchar event_type
        varchar event_status
        varchar result
        text discrepancy_reason
        jsonb details
        timestamp reconciled_at
    }

    SECURITY_USER {
        uuid id PK
        varchar username UK
        varchar password_hash
        varchar role
        boolean enabled
        timestamp created_at
    }
```

### Relational Schema Guarantees
- **Composite Uniqueness on Ingestion**: `UNIQUE(source, event_id)` on `webhook_events`. Providers like Stripe and Shopify may generate identical strings (`evt_123`), but collisions within the same source are rejected.
- **Strict Attempt Numbering**: `UNIQUE(webhook_event_id, attempt_number)` on `webhook_attempts`. Guarantees chronological non-overlapping attempt logs.
- **Reconciliation Audit Uniqueness**: `UNIQUE(webhook_event_id)` on `reconciliation_records` (via Flyway `V4`) to prevent duplicate audits for a single event.

---

## 3. Detailed Request and Processing Flows

### Flow 1: Webhook Ingestion & Idempotency

External producers submit webhooks to `POST /api/v1/webhooks`. Ingestion requires fast acceptance, protection against volumetric overload, and bulletproof deduplication.

```mermaid
sequenceDiagram
    autonumber
    actor Client as HTTP Webhook Client
    participant Ctrl as WebhookController
    participant Limiter as RedisRateLimiter
    participant Svc as WebhookIngestionService
    participant DB as PostgreSQL 16

    Client->>Ctrl: POST /api/v1/webhooks {source, eventId, payload}
    Ctrl->>Limiter: tryAcquire(source) via Redis Lua script
    alt Rate Limit Exceeded
        Limiter-->>Ctrl: rejected (remaining = 0, retryAfter)
        Ctrl-->>Client: 429 Too Many Requests (No DB transaction)
    else Rate Limit OK
        Limiter-->>Ctrl: allowed (limit, remaining)
        Ctrl->>Svc: ingestWebhook(request)
        Svc->>DB: findBySourceAndEventId(source, eventId)
        alt Event Already Exists (Duplicate)
            DB-->>Svc: existing WebhookEvent
            Svc-->>Ctrl: IngestionResult (isNew = false)
            Ctrl-->>Client: 200 OK {"status": "...", "message": "already received"}
        else Event is New
            DB-->>Svc: empty
            Svc->>DB: Single ACID Transaction:<br/>1. save WebhookEvent (status: RECEIVED)<br/>2. save OutboxEvent (status: PENDING)
            DB-->>Svc: committed
            Svc-->>Ctrl: IngestionResult (isNew = true)
            Ctrl-->>Client: 202 Accepted {"status": "RECEIVED", "message": "accepted"}
        end
    end
```

#### Race Condition Handling on Ingestion
If two identical webhooks arrive simultaneously, both might execute `findBySourceAndEventId` before either has committed. Both attempt `saveAndFlush`. The PostgreSQL unique index `UNIQUE(source, event_id)` rejects the second thread with `DataIntegrityViolationException`. `WebhookIngestionService` catches this violation, re-queries the row persisted by the winner, and returns `200 OK`, preserving strict idempotency.

---

### Flow 2: Transactional Outbox & Correlated RabbitMQ Publication

Directly publishing to RabbitMQ during the HTTP request introduces dual-write failures (DB commit succeeds, broker publish fails $\rightarrow$ event lost; or publish succeeds, DB commit fails $\rightarrow$ phantom message). The Transactional Outbox pattern eliminates this vulnerability.

```mermaid
sequenceDiagram
    autonumber
    participant Poller as OutboxPublisher (@Scheduled)
    participant DB as PostgreSQL 16
    participant Pub as WebhookEventPublisher
    participant Broker as RabbitMQ (Exchange: webhook.events)

    loop Polling Interval (e.g. 2000ms)
        Poller->>DB: findPendingEventsForProcessing(status=PENDING, batchSize=10)
        DB-->>Poller: List<OutboxEvent>
        loop For Each OutboxEvent
            Poller->>DB: Re-read status (Guard against concurrent pollers)
            alt Status is still PENDING
                Poller->>Pub: publishEvent(outboxEvent)
                Pub->>Broker: convertAndSend(exchange, routingKey, payload) with CorrelationData
                Broker-->>Pub: Publisher Confirm (ACK / NACK) within timeout (5s)
                alt Confirm ACK
                    Pub-->>Poller: success
                    Poller->>DB: UPDATE outbox_events SET status='PUBLISHED', published_at=now()
                else Confirm NACK or Timeout
                    Pub-->>Poller: failure
                    Poller->>DB: UPDATE outbox_events SET retry_count=retry_count+1, last_error=...
                end
            else Status != PENDING
                Poller-->>Poller: Skip duplicate publication
            end
        end
    end
```

---

### Flow 3: Asynchronous Worker Processing & Attempt History

The consumer listens to `webhook.processing`, acquires an exclusive database lock on the event, and executes business processing.

```mermaid
sequenceDiagram
    autonumber
    participant Broker as RabbitMQ Queue
    participant Consumer as WebhookProcessingConsumer
    participant Svc as WebhookProcessingService
    participant DB as PostgreSQL 16
    participant Processor as WebhookEventProcessor

    Broker->>Consumer: WebhookProcessingMessage (webhookEventId)
    Consumer->>Svc: processEventById(webhookEventId)
    Svc->>DB: findByIdForUpdate(id) [SELECT ... FOR UPDATE]
    DB-->>Svc: WebhookEvent (locked)
    alt Status != RECEIVED (Duplicate Delivery)
        Svc-->>Consumer: Log & Skip (Acknowledge AMQP message)
    else Status == RECEIVED
        Svc->>DB: Update status = PROCESSING, attemptCount = attemptCount + 1
        Svc->>DB: Insert WebhookAttempt (status = PROCESSING, attemptNumber = N)
        Svc->>Processor: processEvent(event)
        alt Processing Succeeded
            Processor-->>Svc: Success
            Svc->>DB: WebhookEvent status = COMPLETED, processedAt = now()
            Svc->>DB: WebhookAttempt status = COMPLETED, completedAt = now()
            Svc-->>Consumer: Done (ACK)
        else Processing Threw Exception
            Processor-->>Svc: Exception
            Svc->>DB: Record failed WebhookAttempt (status = FAILED, error = ...)
            alt attemptCount < maxAttempts
                Svc->>DB: WebhookEvent status = RETRY_PENDING, nextRetryAt = now + delay
            else attemptCount >= maxAttempts
                Svc->>DB: WebhookEvent status = DEAD_LETTER, nextRetryAt = null
            end
            Note over Svc,DB: Committed via @Transactional(noRollbackFor = Exception.class)
            Svc-->>Consumer: Propagate for AMQP DLQ / Reject
        end
    end
```

---

### Flow 4: Retry Scheduling & Exponential Backoff Engine

Failed events enter `RETRY_PENDING` with a deterministically calculated UTC timestamp:
$$d_n = \min(d_0 \cdot m^{n-1}, d_{\max})$$
Worker threads are not held sleeping in memory. Instead, a lightweight scheduler polls PostgreSQL for due events.

```mermaid
sequenceDiagram
    autonumber
    participant Scheduler as WebhookRetryScheduler (@Scheduled)
    participant DB as PostgreSQL 16
    participant TT as TransactionTemplate (Requires New)
    participant Outbox as OutboxEventRepository

    loop Scheduler Interval (e.g. 1000ms)
        Scheduler->>DB: findDueEventsForRetry(status=RETRY_PENDING, nextRetryAt <= now())
        DB-->>Scheduler: List<WebhookEvent>
        loop For Each Due Event
            Scheduler->>TT: execute(claimAndEnqueueRetry)
            TT->>DB: Atomic Claim: UPDATE status RETRY_PENDING -> RECEIVED
            alt Claim Won (1 row updated)
                TT->>Outbox: save(OutboxEvent: status=PENDING, payload=WebhookProcessingMessage)
                TT-->>Scheduler: Committed atomically
            else Claim Lost (0 rows updated)
                TT-->>Scheduler: Skip (claimed by concurrent scheduler node)
            end
        end
    end
```

---

### Flow 5: Dead-Letter Handling, Operator Inspection & Audited Replay

When `attemptCount >= maxAttempts`, automatic retries cease and the event enters `DEAD_LETTER`. Operators inspect the chronological history and safely trigger replays.

```mermaid
sequenceDiagram
    autonumber
    actor Operator as Operator (ROLE_OPERATOR / ROLE_ADMIN)
    participant DLQCtrl as DeadLetterController
    participant DLQSvc as DeadLetterService
    participant DB as PostgreSQL 16
    participant Outbox as OutboxEventRepository

    Operator->>DLQCtrl: GET /api/v1/dead-letters/{id} (Basic Auth)
    DLQCtrl->>DLQSvc: inspectDeadLetter(id)
    DLQSvc->>DB: Load WebhookEvent, WebhookAttempts, WebhookReplays
    DB-->>DLQSvc: Complete history
    DLQSvc-->>DLQCtrl: DeadLetterDetailResponse
    DLQCtrl-->>Operator: 200 OK (Attempt errors, timestamps, reasons)

    Operator->>DLQCtrl: POST /api/v1/dead-letters/{id}/replay {"reason": "Dependency fixed"}
    DLQCtrl->>DLQSvc: replayDeadLetter(id, reason)
    DLQSvc->>DB: Single ACID Transaction:
    Note over DLQSvc,DB: Atomic Conditional Claim:<br/>UPDATE webhook_events SET status='RECEIVED', next_retry_at=NULL<br/>WHERE id=:id AND status='DEAD_LETTER'
    alt Claim Failed (0 rows updated)
        DB-->>DLQSvc: 0 rows (Already replayed or in race)
        DLQSvc-->>DLQCtrl: throw InvalidStateTransitionException
        DLQCtrl-->>Operator: 409 Conflict
    else Claim Won (1 row updated)
        DLQSvc->>DB: Insert WebhookReplay (reason, previousAttemptCount, requestedAt)
        DLQSvc->>Outbox: Insert OutboxEvent (status=PENDING, payload=WebhookProcessingMessage)
        DB-->>DLQSvc: Committed
        DLQSvc-->>DLQCtrl: WebhookReplayResponsePayload
        DLQCtrl-->>Operator: 202 Accepted {"status": "RECEIVED", "message": "enqueued"}
    end
```

---

### Flow 6: Spring Security RBAC & Atomic Redis Rate Limiting

Ingestion traffic is protected against denial-of-service and flooding using distributed fixed-window rate limiting in Redis. Administrative endpoints are protected via stateless Spring Security HTTP Basic authentication.

```mermaid
sequenceDiagram
    autonumber
    actor Client as HTTP Client
    participant Sec as SecurityFilterChain
    participant Ratelimit as RedisRateLimiterService
    participant Redis as Redis 7 (In-Memory)
    participant Endpoint as Controller Endpoint

    Client->>Sec: HTTP Request
    alt Request to /api/v1/webhooks
        Sec-->>Ratelimit: Public Access Allowed -> Execute Rate Limiter
        Ratelimit->>Redis: EVAL Lua Script (INCR key, EXPIRE if 1, TTL)
        Redis-->>Ratelimit: Current Count
        alt Count > Capacity (e.g. 60)
            Ratelimit-->>Client: 429 Too Many Requests (Retry-After: N)
        else Count <= Capacity
            Ratelimit->>Endpoint: Proceed to WebhookController
        end
    else Request to /api/v1/dead-letters/** or /api/v1/reconciliation/**
        alt Missing or Invalid HTTP Basic Credentials
            Sec-->>Client: 401 Unauthorized (WWW-Authenticate: Basic)
        else Valid Credentials, Insufficient Role (e.g. VIEWER on POST /replay)
            Sec-->>Client: 403 Forbidden (RestAccessDeniedHandler)
        else Valid Credentials, Authorized Role (e.g. OPERATOR on POST /replay)
            Sec->>Endpoint: Proceed to Controller Endpoint
        end
    end
```

---

### Flow 7: Reconciliation Workflow against Simulated Source of Truth

Reconciliation runs periodically verify internal processing results against an independent external source of truth.

> [!NOTE]
> The source-of-truth client in this phase is an abstraction (`SourceOfTruthClient`) implemented by `SimulatedSourceOfTruthClient`. It deterministically generates reference states for testing without relying on live third-party network connections.

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Admin / Operator
    participant ReconCtrl as ReconciliationController
    participant ReconSvc as ReconciliationServiceImpl
    participant DB as PostgreSQL 16
    participant Client as SourceOfTruthClient (Simulated)

    Admin->>ReconCtrl: POST /api/v1/reconciliation/runs {"batchSize": 50, "source": "stripe"}
    ReconCtrl->>ReconSvc: runReconciliation(request)
    ReconSvc->>DB: Query eligible terminal events (status IN COMPLETED, DEAD_LETTER, FAILED)
    DB-->>ReconSvc: List<WebhookEvent>
    loop For Each Eligible Event
        Note over ReconSvc,DB: Isolated Transaction Boundary: Propagation.REQUIRES_NEW
        ReconSvc->>DB: findByIdForUpdate(event.getId())
        ReconSvc->>Client: fetchExternalRecord(source, eventId)
        alt External Match
            Client-->>ReconSvc: ExternalEventRecord (status: COMPLETED)
            ReconSvc->>DB: Persist ReconciliationRecord (result = MATCHED)
        else External Confirms Settlement but Local Failed
            Client-->>ReconSvc: ExternalEventRecord (status: SETTLED_WITH_ADJUSTMENT)
            ReconSvc->>DB: Persist ReconciliationRecord (result = CORRECTED, audit snapshot)
        else External Contradicts Local
            Client-->>ReconSvc: ExternalEventRecord (status: FAILED / NOT_FOUND)
            ReconSvc->>DB: Persist ReconciliationRecord (result = MISMATCH, discrepancy reason)
        else Provider Network Outage
            Client-->>ReconSvc: throws ExternalProviderException
            ReconSvc->>DB: Persist ReconciliationRecord (result = FAILED, error detail)
        end
    end
    ReconSvc-->>ReconCtrl: ReconciliationRunResponse (batch statistics)
    ReconCtrl-->>Admin: 200 OK {totalEligible, processedCount, matchedCount, ...}
```

---

## 4. Key Architectural Decisions & Rationale

### 1. Why the Transactional Outbox Pattern?
- **The Problem**: In a distributed system, a single process cannot atomically commit a row to a database and publish a message to a broker without distributed transactions (2PC / XA), which introduce severe latency and single points of failure.
- **The Decision**: Both the `WebhookEvent` and the `OutboxEvent` are written to PostgreSQL in the exact same ACID database transaction.
- **Why It Matters**: If PostgreSQL commits, the outbox record is guaranteed to exist. A background poller then dispatches the message to RabbitMQ with publisher confirms. Even if the application crashes or RabbitMQ is temporarily down, the event will never be lost.

### 2. Why At-Least-Once Delivery + Consumer Idempotency?
- **The Problem**: Distributed network partitions and acknowledgements make "exactly-once delivery" physically impossible across message brokers. If a worker finishes processing but crashes before acknowledging RabbitMQ, the broker will redeliver the message.
- **The Decision**: Accept at-least-once message delivery and enforce strict idempotency at the database boundary.
- **Why It Matters**: Workers inspect the authoritative status of the event under a row-level lock (`SELECT ... FOR UPDATE`). If the event is already `COMPLETED`, `PROCESSING`, `RETRY_PENDING`, or `DEAD_LETTER`, the worker acknowledges the AMQP delivery and skips execution. Exactly-once *processing semantics* are guaranteed without relying on unsupported broker guarantees.

### 3. Why Flyway Migrations & `ddl-auto: validate`?
- **The Problem**: Allowing Hibernate to auto-generate or alter database schemas (`ddl-auto: update`) in production causes silent schema divergence, lock contention, and untracked changes across cluster nodes.
- **The Decision**: Flyway executes immutable, versioned SQL scripts (`V1` through `V4`) during startup. Hibernate is configured strictly to `validate`.
- **Why It Matters**: Schema modifications are versioned in Git, auditable, reproducible, and tested identically across development, CI, and production.

### 4. Why Database Concurrency Controls (Row Locks & Constraints)?
- **The Problem**: Distributed workers running concurrently can pull duplicate copies of the same message or duplicate replay requests simultaneously, resulting in dual executions and inconsistent attempt counts.
- **The Decision**: Combine three distinct concurrency safeguards:
  1. **Relational Unique Constraints**: `UNIQUE(source, event_id)`, `UNIQUE(webhook_event_id, attempt_number)`, and `UNIQUE(webhook_event_id)`.
  2. **Pessimistic Row Locks (`SELECT ... FOR UPDATE`)**: In `WebhookProcessingService` and `ReconciliationServiceImpl`, serialize concurrent executions targeting the same webhook event.
  3. **Atomic Conditional SQL Updates**: In `WebhookRetryScheduler` and `DeadLetterService`, execute `UPDATE ... WHERE id = :id AND status = :expectedStatus`. Exactly one thread modifies the row; losing threads observe 0 updated rows and back off.

### 5. Why Isolated Transaction Boundaries (`TransactionTemplate` & `REQUIRES_NEW`)?
- **The Problem**:
  - In `WebhookProcessingService`, catching an exception, updating state to `RETRY_PENDING`, and then throwing an exception would normally trigger a Spring rollback, discarding the failure attempt.
  - In `WebhookRetryScheduler` and `ReconciliationServiceImpl`, internal helper methods called on `this` bypass Spring AOP transaction interceptors, causing `TransactionRequiredException` or shared transaction failure cascades.
- **The Decision**:
  - Configured `WebhookProcessingService` with `@Transactional(noRollbackFor = Exception.class)`.
  - Injected `PlatformTransactionManager` into schedulers and reconciliation services to execute discrete units of work within explicit `TransactionTemplate` instances with `PROPAGATION_REQUIRES_NEW`.
- **Why It Matters**: A failure on a single webhook during a reconciliation run commits its own `FAILED` record without rolling back or poisoning the surrounding batch.

---

## 5. Infrastructure & Resilience Components

| Component | Technology | Configuration & Resilience Features |
|---|---|---|
| **Primary Database** | PostgreSQL 16 | HikariCP connection pool (min: 2, max: 10), connection timeout 20s, UTC timestamps (`TIMESTAMP WITH TIME ZONE`), JSONB native columns. |
| **Message Broker** | RabbitMQ 3.13 | Correlated publisher confirms (`publisher-confirm-type: correlated`), publisher returns enabled, durable direct exchange (`webhook.events`), durable queue (`webhook.processing`). |
| **Distributed Cache** | Redis 7 | Standalone / Cluster support, atomic Lua scripts, configurable fail-closed policy (`RATE_LIMIT_FAIL_CLOSED=true`) returning HTTP 503 on outages. |
| **Application Runtime** | Spring Boot 3.4 / Java 21 | Graceful shutdown (`server.shutdown: graceful`, 30s timeout per phase), stateless HTTP sessions, health probes via Spring Actuator. |

---

## 6. Known Limitations & Architecture Roadmap

- **Simulated Source of Truth**: The reconciliation engine uses `SimulatedSourceOfTruthClient`. Production deployments require concrete adapters implementing `SourceOfTruthClient` for each provider (e.g., Stripe, PayPal, Shopify).
- **Cryptographic Provider Signatures**: The public ingestion endpoint validates JSON payloads and enforces Redis rate limiting, but does not yet verify cryptographic provider HMAC SHA-256 signatures (scheduled for Phase 11).
- **Non-Mutating `CORRECTED` Audits**: When reconciliation identifies an external settlement for a locally failed event, it records a `CORRECTED` audit record but preserves the internal event status to maintain historical immutability.
