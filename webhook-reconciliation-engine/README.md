# Webhook Reconciliation Engine

## Description

The Webhook Reconciliation Engine is a reliable, production-grade webhook processing platform designed to receive, validate, persist, and process webhook events from external services.

The system guarantees idempotent processing of webhook events, uses asynchronous message-driven processing with RabbitMQ, and provides automated reconciliation between internal state and external sources of truth. Failed events are retried with exponential backoff and moved to a Dead Letter Queue after permanent failures, with the ability to replay them.

The platform is built for observability, providing health checks, metrics, and an administrative dashboard for monitoring and managing webhook processing.

## Current Status

> **Phase 8 — Reconciliation Engine (Complete)**
>
> Automated reconciliation workflow comparing locally processed webhook events against an independent source of truth abstraction (`SourceOfTruthClient` with deterministic simulated provider `SimulatedSourceOfTruthClient`). Persists auditable reconciliation records classified into `MATCHED`, `CORRECTED`, `MISMATCH`, and `FAILED` with detailed JSON comparison snapshots. Bounded batch execution (`POST /api/v1/reconciliation/runs`), paginated listing with status/source filters (`GET /api/v1/reconciliation/records`), and record detail inspection (`GET /api/v1/reconciliation/records/{id}`) are secured with Spring Security RBAC (`ADMIN` and `OPERATOR` can initiate runs; `VIEWER` can view records). Concurrency protection is enforced via Flyway V4 database uniqueness constraints (`uk_reconciliation_records_event_id`) and per-event isolated transactions (`REQUIRES_NEW`).

## Architecture & Messaging Pipeline

### Transactional Outbox, Retry & Dead-Letter Pipeline
```text
HTTP Client
   ↓ POST /api/v1/webhooks
WebhookController (Validation)
   ↓
WebhookIngestionService
   │
   ├── [Single PostgreSQL Transaction]
   │   ├── Persist WebhookEvent (status: RECEIVED)
   │   └── Persist OutboxEvent (status: PENDING)
   │
   └── Return 202 Accepted immediately

[Outbox Publisher Worker]
OutboxPublisher (@Scheduled polling)
   ↓ Query OutboxEventRepository (status = PENDING)
WebhookEventPublisher
   ↓ RabbitMQ publish with Publisher Confirms
   │ (exchange: webhook.events, routingKey: webhook.received)
   │
   ├── If ACK: OutboxEvent marked PUBLISHED
   └── If NACK/failure: OutboxEvent remains PENDING (retryCount++)

RabbitMQ Queue: webhook.processing
   ↓
WebhookProcessingConsumer -> WebhookProcessingService (Idempotent execution)
   │
   ├── Processing Success: WebhookEvent -> COMPLETED, WebhookAttempt -> COMPLETED
   │
   └── Processing Failure:
       ├── Record failed WebhookAttempt (status: FAILED, error message, completedAt)
       │
       ├── If attemptCount < maxAttempts:
       │   ├── Calculate delay: d_n = min(d_0 * m^(n-1), d_max)
       │   └── WebhookEvent -> RETRY_PENDING, nextRetryAt = now + delay
       │
       └── If attemptCount >= maxAttempts:
           └── WebhookEvent -> DEAD_LETTER (retries exhausted, nextRetryAt = null)

[Retry Scheduler Worker]
WebhookRetryScheduler (@Scheduled polling)
   ↓ Query due events (status = RETRY_PENDING, nextRetryAt <= now)
   ↓ Atomic claim (UPDATE status RETRY_PENDING -> RECEIVED)
   └── Write new OutboxEvent (status: PENDING) in the same PostgreSQL transaction
         ↓
       OutboxPublisher dispatches to RabbitMQ for re-execution

[Operator Dead-Letter Replay]
Operator / Admin Client
   ↓ POST /api/v1/dead-letters/{id}/replay
DeadLetterController -> DeadLetterService
   │
   ├── [Single PostgreSQL Transaction]
   │   ├── Atomic Claim: UPDATE status DEAD_LETTER -> RECEIVED (fails with 409 if race lost)
   │   ├── Clear nextRetryAt = null, preserve attemptCount and historical WebhookAttempt records
   │   ├── Persist audit record in webhook_replays (reason, previousAttemptCount, requestedAt)
   │   └── Persist OutboxEvent (status: PENDING, aggregateId = id, payload = WebhookProcessingMessage)
   │
   └── Return 202 Accepted (OutboxPublisher dispatches to RabbitMQ for re-execution)
```

### Exponential Backoff Policy & Configuration
Exponential backoff formula:
$$d_n = \min(d_0 \times m^{n-1}, d_{\max})$$

Where:
- $d_n$: Delay duration before next execution
- $d_0$: Initial delay (default: `1000ms`)
- $m$: Multiplier (default: `2.0`)
- $n$: Failed execution attempt number ($1, 2, 3\dots$)
- $d_{\max}$: Maximum delay cap (default: `60000ms`)

#### Configuration (`application.yml`)
```yaml
app:
  retry:
    enabled: ${RETRY_ENABLED:true}
    max-attempts: ${RETRY_MAX_ATTEMPTS:5}
    initial-delay-ms: ${RETRY_INITIAL_DELAY_MS:1000}
    multiplier: ${RETRY_MULTIPLIER:2.0}
    max-delay-ms: ${RETRY_MAX_DELAY_MS:60000}
    scheduler-interval-ms: ${RETRY_SCHEDULER_INTERVAL_MS:1000}
    batch-size: ${RETRY_BATCH_SIZE:20}
```

#### Attempt Counting Convention
`max-attempts` represents the maximum number of automatic processing executions, including the initial execution.
With default `max-attempts: 5`:
- **Attempt 1 fails**: $d_1 = 1000 \times 2^0 = 1000\text{ms}$ (1s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 2 fails**: $d_2 = 1000 \times 2^1 = 2000\text{ms}$ (2s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 3 fails**: $d_3 = 1000 \times 2^2 = 4000\text{ms}$ (4s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 4 fails**: $d_4 = 1000 \times 2^3 = 8000\text{ms}$ (8s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 5 fails**: Maximum attempts reached (5 executions completed) $\rightarrow$ `DEAD_LETTER`

### Event Lifecycle & Transitions
1. **Initial Acceptance**: `RECEIVED` / `PENDING` (Outbox)
2. **First Execution**: `PROCESSING` $\rightarrow$ `COMPLETED` (Success) OR `RETRY_PENDING` (Retryable failure)
3. **Delayed Re-execution**: `RETRY_PENDING` $\rightarrow$ Claimed by scheduler as `RECEIVED` $\rightarrow$ `PROCESSING`
4. **Retry Exhaustion**: `PROCESSING` $\rightarrow$ `DEAD_LETTER` (when attempt count reaches `max-attempts`, `nextRetryAt = null`)
5. **Operator Replay**: `DEAD_LETTER` $\rightarrow$ Atomically claimed as `RECEIVED` $\rightarrow$ `PROCESSING` $\rightarrow$ `COMPLETED` (or `DEAD_LETTER` if replayed execution fails again)

### Difference Between FAILED and DEAD_LETTER
- **`FAILED`**: Denotes irreversible transport/publication terminal errors (e.g. invalid JSON payloads on outbox events, or legacy failed states). Previously marked terminal failures remain intact and are not deleted.
- **`DEAD_LETTER`**: Denotes webhook events that have exhausted their automatic retry budget during business processing. They halt automatic scheduler retries, preserve their entire attempt audit trail, and are specifically queryable and replayable by operators.

### Attempt Numbers & Retry Limits After Manual Replay
- **Attempt History Preservation**: Existing `WebhookAttempt` records are never overwritten or deleted.
- **Unique Constraint Compatibility**: The database constraint `UNIQUE(webhook_event_id, attempt_number)` is respected because the next processing execution claims attempt number $N+1$ (where $N = \text{event.getAttemptCount()}$).
- **Retry Policy Behavior After Replay**: Because $N+1 > \text{maxAttempts}$, if a replayed event fails again, `retryPolicy.canRetry(N+1)` evaluates to `false`. This prevents infinite automatic retry loops. If the replayed run fails, it returns immediately to `DEAD_LETTER`, awaiting further operator investigation. If the replayed run succeeds, it transitions to `COMPLETED`. Operator intervention is always deliberate and guarded.

### Publication vs. Processing Failure Separation
- **Publication Failure**: RabbitMQ is unavailable when `OutboxPublisher` attempts delivery. The `OutboxEvent` remains `PENDING` with its `retryCount` incremented. The `WebhookEvent` attempt count is **not** incremented because business processing has not occurred.
- **Processing Failure**: RabbitMQ delivered the message successfully, but `WebhookEventProcessor` threw an exception. A new failed `WebhookAttempt` is recorded, the event's `attemptCount` is incremented, and `WebhookEvent` transitions to `RETRY_PENDING` or `DEAD_LETTER`. The outbox publication record remains marked as `PUBLISHED`.

### Duplicate Message & Concurrency Protection
- **COMPLETED Events**: If a duplicate message arrives for an already `COMPLETED` event, the worker detects this via database query and skips execution without re-processing.
- **Active PROCESSING Events**: If duplicate concurrent messages arrive, the status check prevents redundant execution.
- **Concurrent Replays**: The replay operation uses an atomic conditional SQL update (`UPDATE webhook_events SET status = 'RECEIVED', next_retry_at = NULL WHERE id = :id AND status = 'DEAD_LETTER'`). If multiple concurrent replay requests arrive, exactly one thread updates a row and proceeds to enqueue an outbox message; the others receive 0 updated rows and return `409 Conflict`.
- **At-Least-Once Delivery**: Outbox publishing guarantees at-least-once delivery to RabbitMQ even if the application crashes or RabbitMQ experiences temporary network degradation. Worker idempotency safeguards against duplicates.

## Webhook Ingestion API

### Ingest Webhook Event

- **Endpoint**: `POST /api/v1/webhooks`
- **Content-Type**: `application/json`

#### Validation Rules
- `eventId`: Required, non-blank string (maximum 255 characters).
- `source`: Required, non-blank string (maximum 100 characters).
- `eventType`: Required, non-blank string (maximum 100 characters).
- `payload`: Required, valid JSON object (arbitrary schema supported).

#### Sample Request

```json
{
  "eventId": "evt_payment_123",
  "source": "payment-service",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "pay_456",
    "amount": 1499.00,
    "currency": "INR",
    "status": "COMPLETED"
  }
}
```

#### Responses

- **New Event (`202 Accepted`)**:
```json
{
  "eventId": "evt_payment_123",
  "source": "payment-service",
  "status": "RECEIVED",
  "message": "Webhook accepted for processing"
}
```

- **Duplicate Event (`200 OK`)**:
```json
{
  "eventId": "evt_payment_123",
  "source": "payment-service",
  "status": "RECEIVED",
  "message": "Webhook event already received"
}
```

- **Validation Failure (`400 Bad Request`)**:
```json
{
  "status": 400,
  "error": "Validation Failed",
  "message": "Request validation failed for one or more fields",
  "details": [
    "eventId is required and cannot be blank"
  ],
  "timestamp": "2026-10-09T11:45:00.000Z"
}
```

#### Idempotency Behavior
1. **Uniqueness Scope**: Events are uniquely scoped by `(source, eventId)`.
2. **Provider Isolation**: Identical event IDs from different sources (e.g., `stripe` and `github`) are treated as independent events.
3. **No Overwrites**: Duplicate submissions will never mutate original payloads, statuses, attempt counts, or timestamps.
4. **Concurrency Safety**: Both sequential duplicates and concurrent race conditions are protected via the PostgreSQL `UNIQUE(source, event_id)` constraint, gracefully catching and returning the stored record.

## Dead-Letter & Replay API

Endpoints under `/api/v1/dead-letters` allow operators to inspect exhausted events and safely trigger re-executions through the Transactional Outbox.

### 1. List Dead-Letter Events

- **Endpoint**: `GET /api/v1/dead-letters`
- **Query Parameters**:
  - `page` (default: 0): Page index (0-based)
  - `size` (default: 20): Page size
  - `sort` (default: `createdAt,desc`): Sort field and direction
- **Responses**: `200 OK`

#### Sample curl
```bash
curl -X GET "http://localhost:8080/api/v1/dead-letters?page=0&size=10" \
  -H "Accept: application/json"
```

#### Sample Response (`200 OK`)
```json
{
  "content": [
    {
      "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "eventId": "evt_charge_failed_999",
      "source": "stripe",
      "eventType": "payment_intent.payment_failed",
      "status": "DEAD_LETTER",
      "attemptCount": 5,
      "lastError": "Connection timed out connecting to payment service",
      "createdAt": "2026-10-11T12:00:00Z",
      "updatedAt": "2026-10-11T12:05:00Z"
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 10
  },
  "totalElements": 1,
  "totalPages": 1
}
```

### 2. Inspect Single Dead-Letter Event

- **Endpoint**: `GET /api/v1/dead-letters/{id}`
- **Path Variable**: `id` (UUID of the webhook event)
- **Responses**:
  - `200 OK`: Successful lookup with chronological attempt history and replay records.
  - `404 Not Found`: No event found with the given UUID.
  - `409 Conflict`: Event exists but is not in `DEAD_LETTER` state.

#### Sample curl
```bash
curl -X GET "http://localhost:8080/api/v1/dead-letters/3fa85f64-5717-4562-b3fc-2c963f66afa6" \
  -H "Accept: application/json"
```

#### Sample Response (`200 OK`)
```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "eventId": "evt_charge_failed_999",
  "source": "stripe",
  "eventType": "payment_intent.payment_failed",
  "status": "DEAD_LETTER",
  "attemptCount": 5,
  "lastError": "Connection timed out connecting to payment service",
  "createdAt": "2026-10-11T12:00:00Z",
  "updatedAt": "2026-10-11T12:05:00Z",
  "nextRetryAt": null,
  "attempts": [
    {
      "id": "b0f7e4e1-7a6c-4b68-b7c1-15bce6f2db01",
      "attemptNumber": 1,
      "status": "FAILED",
      "startedAt": "2026-10-11T12:00:01Z",
      "completedAt": "2026-10-11T12:00:02Z",
      "errorMessage": "503 Service Unavailable"
    },
    {
      "id": "c1f7e4e1-7a6c-4b68-b7c1-15bce6f2db02",
      "attemptNumber": 2,
      "status": "FAILED",
      "startedAt": "2026-10-11T12:00:03Z",
      "completedAt": "2026-10-11T12:00:04Z",
      "errorMessage": "Connection timed out connecting to payment service"
    }
  ],
  "replays": []
}
```

### 3. Replay Dead-Letter Event

- **Endpoint**: `POST /api/v1/dead-letters/{id}/replay`
- **Path Variable**: `id` (UUID of the webhook event)
- **Request Body**: Optional JSON containing `reason` (max 500 characters).
- **Responses**:
  - `202 Accepted`: Replay successfully accepted and enqueued through outbox.
  - `400 Bad Request`: Validation failure on request body (e.g., reason > 500 chars).
  - `404 Not Found`: Event not found.
  - `409 Conflict`: Event is not in `DEAD_LETTER` status, or concurrent replay claimed it first.

#### Sample curl
```bash
curl -X POST "http://localhost:8080/api/v1/dead-letters/3fa85f64-5717-4562-b3fc-2c963f66afa6/replay" \
  -H "Content-Type: application/json" \
  -d '{"reason": "Transient dependency issue has been resolved"}'
```

#### Sample Response (`202 Accepted`)
```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "eventId": "evt_charge_failed_999",
  "source": "stripe",
  "status": "RECEIVED",
  "message": "Webhook event replay accepted and enqueued for processing"
}
```

### How to Test Replay End-to-End

1. **Ingest a webhook** via `POST /api/v1/webhooks`.
2. **Trigger repeated failures** until attempt count reaches `max-attempts: 5`. Verify the event transitions to `DEAD_LETTER` and `next_retry_at` is `NULL`.
3. **List dead-letters** via `GET /api/v1/dead-letters` and inspect the failed event details via `GET /api/v1/dead-letters/{id}`.
4. **Trigger replay** via `POST /api/v1/dead-letters/{id}/replay` with an optional reason.
5. **Verify Outbox & Processing**: The event transitions to `RECEIVED`, an outbox record is inserted with `status = 'PENDING'`, and a row is appended to `webhook_replays`. `OutboxPublisher` pushes the message to RabbitMQ `webhook.processing`, and the worker consumes the message, incrementing `attemptNumber` to 6.

## Security & Role-Based Access Control (RBAC)

### 1. Authentication Strategy
The operational API endpoints are protected using Spring Security with HTTP Basic authentication and stateless sessions (`SessionCreationPolicy.STATELESS`). Client credentials are authenticated against operational accounts stored in PostgreSQL table `security_users` with strong BCrypt password hashing.

- **Password Storage**: Passwords are encrypted with BCrypt (`BCryptPasswordEncoder`). Plaintext passwords are never persisted, returned in API responses, or printed in logs.
- **Session Management**: Completely stateless; no HTTP sessions or cookies are created.
- **Error Responses**: Missing or invalid credentials return HTTP `401 Unauthorized` with a `WWW-Authenticate: Basic realm="Webhook Reconciliation Engine"` challenge and structured JSON `ErrorResponse`. Insufficient permissions return HTTP `403 Forbidden` with a structured JSON `ErrorResponse`.

### 2. Role-Permission Matrix

| Endpoint | Method | ADMIN | OPERATOR | VIEWER | Unauthenticated | Description |
|---|---|---|---|---|---|---|
| `/api/v1/webhooks` | POST | Allow | Allow | Allow | Allow | Public ingestion endpoint, subject to Redis rate limiting |
| `/api/v1/dead-letters` | GET | Allow | Allow | Allow | 401 | List dead-letter webhook events |
| `/api/v1/dead-letters/{id}` | GET | Allow | Allow | Allow | 401 | Inspect single dead-letter event details |
| `/api/v1/dead-letters/{id}/replay` | POST | Allow | Allow | 403 | 401 | Replay dead-letter event through outbox |
| `/api/v1/reconciliation/runs` | POST | Allow | Allow | 403 | 401 | Initiate bounded reconciliation run |
| `/api/v1/reconciliation/records` | GET | Allow | Allow | Allow | 401 | Paginated list of reconciliation records with optional filters |
| `/api/v1/reconciliation/records/{id}` | GET | Allow | Allow | Allow | 401 | Inspect individual reconciliation record details |
| `/actuator/health` | GET | Allow | Allow | Allow | Allow | Actuator health endpoint |
| `/api-docs/**`, `/swagger-ui/**` | GET | Allow | Allow | Allow | Allow | OpenAPI and Swagger UI documentation |
| *All other application endpoints* | ANY | Check | Check | Check | 401 | Protected by default |

### 3. Public Webhook Ingestion Trust Boundary
- The ingestion endpoint (`POST /api/v1/webhooks`) is intended for external webhook producers and allows unauthenticated requests.
- **Trust Boundary Warning**: The `source` field in the webhook body is client-provided and can be spoofed in this phase (provider cryptographic signatures are not evaluated in Phase 7).
- Ingestion traffic is strictly regulated using Redis rate limiting to protect internal databases and message brokers against flooding and bursts.

### 4. Bootstrap Administrator Setup
The initial administrator account is bootstrapped safely on startup via environment variables:
- `APP_SECURITY_BOOTSTRAP_USERNAME`
- `APP_SECURITY_BOOTSTRAP_PASSWORD`

Bootstrap guarantees:
- **Disabled when unset**: If either environment variable is empty or null, bootstrapping is skipped entirely.
- **Idempotent**: If the specified administrator already exists in PostgreSQL, startup checks `existsByUsername` and skips creation without overwriting or resetting the existing password hash.
- **No plaintext logging**: The plaintext password is never logged during bootstrapping.
- **No hardcoded secrets**: Credentials are kept in `.env` (git-ignored) with placeholders only in `.env.example`.

## Redis Rate Limiting

### 1. Algorithm & Atomicity
Webhook ingestion is protected by a distributed fixed-window rate limiter backed by Redis.
- **Atomic Lua Script**: The rate limit check executes an atomic Lua script in Redis (`INCR` followed by `EXPIRE` on first increment, returning the current count and ensuring TTL is always set).
- **Why Atomic**: Redis executes Lua scripts in a single atomic thread; no concurrent request or command can run between `INCR` and `EXPIRE`. This eliminates the classic race condition where counters could be incremented without a reliable expiry.
- **Fixed Epoch Window**: Requests within `[windowEpoch * windowSeconds, (windowEpoch + 1) * windowSeconds)` are coordinated by epoch-partitioned Redis keys, ensuring counter resets at window boundaries and automatic TTL eviction (`expireSeconds = windowSeconds * 2`).

### 2. Key Strategy & Bounded Normalization
- Redis Key: `app:ratelimit:src:<safeSource>:<epochWindow>`
- Normalization:
  - Raw `source` is trimmed and converted to lowercase.
  - Safe alphanumeric strings (`^[a-z0-9_-]{1,64}$`) are used directly.
  - Strings containing special characters or exceeding safe length are hashed using SHA-256 (`hash_<sha256Hex>`), ensuring Redis keys remain strictly bounded and immune to key injection.
  - Missing or blank source values fallback safely to `"unknown"`.

### 3. Configuration Properties

| Property | Environment Variable | Default | Description |
|---|---|---|---|
| `app.rate-limit.enabled` | `RATE_LIMIT_ENABLED` | `true` | Toggle rate limiting globally |
| `app.rate-limit.capacity` | `RATE_LIMIT_CAPACITY` | `60` | Max requests permitted per source per window |
| `app.rate-limit.window-seconds` | `RATE_LIMIT_WINDOW_SECONDS` | `60` | Window size in seconds |
| `app.rate-limit.key-strategy` | `RATE_LIMIT_KEY_STRATEGY` | `source` | Key strategy (source-based) |
| `app.rate-limit.fail-closed` | `RATE_LIMIT_FAIL_CLOSED` | `true` | Fail-closed policy on Redis outages |

### 4. Response Codes & Headers
- **Allowed (202 / 200)**: Response includes `X-RateLimit-Limit` and `X-RateLimit-Remaining`.
- **Exceeded Limit (429 Too Many Requests)**: Response includes `Retry-After: <secondsRemaining>`, `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, and a structured `ErrorResponse` JSON.
- **Transactional Guarantee**: Rejected requests return immediately without creating or persisting `WebhookEvent` or `OutboxEvent` records. Phase 2 idempotency remains preserved for allowed requests.

### 5. Redis Outage Behavior (Fail-Closed Policy)
- When Redis is unavailable or times out, the rate limiter evaluates the configurable `fail-closed` property (default: `true`).
- **Fail-Closed (Default)**: Ingestion fails closed with HTTP `503 Service Unavailable`, `Retry-After: 30`, and a generic error message. No internal Redis exception details or stack traces are leaked to the client. This prevents unthrottled traffic spikes from overwhelming the system during Redis outages.
- **Fail-Open (Configurable)**: If `fail-closed: false` is configured, outages log a warning and allow ingestion to proceed, prioritizing availability over throttling enforcement.

## Reconciliation Engine

The Reconciliation Engine provides an automated and auditable reconciliation workflow comparing internal webhook processing state against an independent external source of truth.

### 1. Source-of-Truth Abstraction & Simulated Provider
- **Abstraction (`SourceOfTruthClient`)**: Decouples the reconciliation engine from concrete external API implementations. Exposes `Optional<ExternalEventRecord> fetchExternalRecord(String source, String eventId) throws ExternalProviderException`.
- **Deterministic Simulated Provider (`SimulatedSourceOfTruthClient`)**: Used for local development and automated testing without external dependencies:
  - `*error*` / `*_error` in `eventId`: Simulates upstream network or provider errors (`ExternalProviderException`).
  - `*notfound*` / `*_notfound` in `eventId`: Simulates missing records on the external source of truth (`Optional.empty()`).
  - `*mismatch*` / `*_mismatch` in `eventId`: Returns contradictory reference status (e.g., local `COMPLETED` vs. external `FAILED`).
  - `*corrected*` / `*_corrected` in `eventId`: Returns external `COMPLETED` when internal status is `DEAD_LETTER` or `FAILED`.
  - Normal event IDs: Deterministically returns matching status (`COMPLETED` for `COMPLETED`, `FAILED` for `FAILED` / `DEAD_LETTER`).
  - Programmatic test overrides: Supports `registerRecord()`, `registerFailure()`, and `clearOverrides()`.

### 2. Event Eligibility Rules
Reconciliation runs evaluate webhook events against strict eligibility criteria:
1. **Terminal Status**: Only events in terminal or exhausted states (`COMPLETED`, `DEAD_LETTER`, `FAILED`) are eligible. Active inflight events (`RECEIVED`, `PROCESSING`, `RETRY_PENDING`) are deliberately skipped to prevent race conditions with active processing pipelines.
2. **Unreconciled Only**: By default, only unreconciled events are selected (no corresponding record in `reconciliation_records`).
3. **Bounded Processing**: Queries use database pagination (`PageRequest.of(0, batchSize)`) bounded by `app.reconciliation.max-batch-size` (default: 100).
4. **Source Filtering**: Batch runs can optionally filter eligibility by webhook `source`.

### 3. Classification Logic & Outcomes

| Reconciliation Result | Condition | Webhook State Mutation | Description |
|---|---|---|---|
| `MATCHED` | External status strictly matches local status (`COMPLETED` == `COMPLETED`/`SUCCESS`, or `FAILED`/`DEAD_LETTER` == `FAILED`/`ERROR`) | None | Internal processing matches external source of truth. |
| `CORRECTED` | External status is `COMPLETED` while local status is `DEAD_LETTER` or `FAILED` | **No status rewrite** (Preserved) | External source confirmed business success despite local failure. Recorded with detailed discrepancy explanation without silently mutating webhook lifecycle history. |
| `MISMATCH` | External event not found (`NOT_FOUND`), or conflicting statuses (e.g., local `COMPLETED` but external `FAILED`) | None | Local state contradicts external source of truth, requiring operator investigation. |
| `FAILED` | Provider error or network timeout thrown during record retrieval | None | Upstream communication failure. Recorded per event without failing or aborting the overall batch. |

### 4. Concurrency & Deduplication Guarantees
- **Database Unique Constraint**: `uk_reconciliation_records_event_id` on `reconciliation_records(webhook_event_id)` enforced at the database level via Flyway `V4`.
- **Isolated Per-Event Transactions**: Each reconciliation comparison runs in an isolated `REQUIRES_NEW` transaction boundary. A failure or unique constraint violation on one event rolls back only that event's transaction and does not abort or contaminate the batch.
- **Race Condition Handling**: If concurrent reconciliation runs evaluate the same webhook event, exactly one transaction succeeds; concurrent attempts catch `DataIntegrityViolationException`, log a warning, and safely skip duplicate creation.

### 5. Reconciliation API Endpoints

#### Initiate Bounded Reconciliation Run
- **Endpoint**: `POST /api/v1/reconciliation/runs`
- **Authorization**: `ADMIN`, `OPERATOR`
- **Request Body**:
  ```json
  {
    "batchSize": 25,
    "source": "stripe",
    "forceReconcile": false
  }
  ```
- **Response (`200 OK`)**:
  ```json
  {
    "totalEligible": 3,
    "processedCount": 3,
    "matchedCount": 2,
    "mismatchCount": 1,
    "correctedCount": 0,
    "failedCount": 0,
    "records": [
      {
        "id": "7b0d77f2-1d59-4ee4-9441-26c7d1e8ef90",
        "webhookEventId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        "eventId": "evt_charge_001",
        "source": "stripe",
        "eventType": "payment_intent.succeeded",
        "result": "MATCHED",
        "discrepancyReason": null,
        "reconciledAt": "2026-10-11T20:30:00Z"
      }
    ]
  }
  ```

#### List Reconciliation Records
- **Endpoint**: `GET /api/v1/reconciliation/records?page=0&size=20&result=MISMATCH&source=stripe`
- **Authorization**: `ADMIN`, `OPERATOR`, `VIEWER`
- **Response (`200 OK`)**: Paginated JSON list of reconciliation record summaries.

#### Inspect Reconciliation Record Detail
- **Endpoint**: `GET /api/v1/reconciliation/records/{id}`
- **Authorization**: `ADMIN`, `OPERATOR`, `VIEWER`
- **Response (`200 OK`)**:
  ```json
  {
    "id": "7b0d77f2-1d59-4ee4-9441-26c7d1e8ef90",
    "webhookEventId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "eventId": "evt_charge_002",
    "source": "stripe",
    "eventType": "payment_intent.succeeded",
    "eventStatus": "DEAD_LETTER",
    "result": "CORRECTED",
    "discrepancyReason": "External source of truth indicates COMPLETED while local status was DEAD_LETTER",
    "details": {
      "localStatus": "DEAD_LETTER",
      "externalStatus": "COMPLETED",
      "externalEventId": "evt_charge_002",
      "externalSource": "stripe",
      "checkedAt": "2026-10-11T20:30:00Z",
      "note": "Local event remains DEAD_LETTER; external system confirms business completion"
    },
    "reconciledAt": "2026-10-11T20:30:00Z"
  }
  ```

## Verification & Manual Testing Commands

### 1. Test Ingestion with Rate Limiting (Public Endpoint)

```bash
# Ingest webhook within capacity (returns 202 with X-RateLimit-* headers)
curl -i -X POST http://localhost:8080/api/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "evt_test_001",
    "source": "stripe",
    "eventType": "payment.succeeded",
    "payload": {"amount": 1000}
  }'

# Exhaust rate limit capacity (send requests until capacity is exceeded)
# Exceeded request returns 429 Too Many Requests with Retry-After header:
# HTTP/1.1 429 Too Many Requests
# Retry-After: 48
# X-RateLimit-Limit: 60
# X-RateLimit-Remaining: 0
```

### 2. Verify Authentication on Operational APIs

```bash
# Unauthenticated request to dead-letter list (returns 401 Unauthorized)
curl -i -X GET http://localhost:8080/api/v1/dead-letters

# Request with invalid credentials (returns 401 Unauthorized)
curl -i -X GET http://localhost:8080/api/v1/dead-letters \
  -u "admin:wrongpassword"

# Request with valid administrator credentials (returns 200 OK)
curl -i -X GET http://localhost:8080/api/v1/dead-letters \
  -u "admin:your_secure_admin_password_here"
```

### 3. Verify Role Authorization (Viewer vs Operator)

```bash
# VIEWER inspecting dead-letter details (returns 200 OK)
curl -i -X GET http://localhost:8080/api/v1/dead-letters/{eventId} \
  -u "viewer:viewer_password"

# VIEWER attempting replay (returns 403 Forbidden)
curl -i -X POST http://localhost:8080/api/v1/dead-letters/{eventId}/replay \
  -u "viewer:viewer_password" \
  -H "Content-Type: application/json" \
  -d '{"reason": "Unauthorized attempt"}'

# OPERATOR or ADMIN executing replay (returns 202 Accepted)
curl -i -X POST http://localhost:8080/api/v1/dead-letters/{eventId}/replay \
  -u "operator:operator_password" \
  -H "Content-Type: application/json" \
  -d '{"reason": "Service dependency restored"}'
```

### 4. Verify Reconciliation Workflow (Runs & Records)

```bash
# VIEWER listing reconciliation records (returns 200 OK)
curl -i -X GET "http://localhost:8080/api/v1/reconciliation/records?page=0&size=10" \
  -u "viewer:viewer_password"

# Filter records by classification result (e.g. MISMATCH)
curl -i -X GET "http://localhost:8080/api/v1/reconciliation/records?result=MISMATCH" \
  -u "viewer:viewer_password"

# VIEWER attempting to initiate a reconciliation run (returns 403 Forbidden)
curl -i -X POST http://localhost:8080/api/v1/reconciliation/runs \
  -u "viewer:viewer_password" \
  -H "Content-Type: application/json" \
  -d '{"batchSize": 25}'

# OPERATOR or ADMIN initiating a reconciliation run (returns 200 OK)
curl -i -X POST http://localhost:8080/api/v1/reconciliation/runs \
  -u "operator:operator_password" \
  -H "Content-Type: application/json" \
  -d '{"batchSize": 50, "source": "stripe"}'

# Inspect individual reconciliation record details with comparison audit (returns 200 OK)
curl -i -X GET http://localhost:8080/api/v1/reconciliation/records/{recordId} \
  -u "viewer:viewer_password"
```

### 5. How to Reset Local Test Data Safely

```bash
# Stop application and tear down docker containers and data volumes:
docker compose down -v

# Start clean infrastructure containers:
docker compose up -d

# Flyway will recreate all tables cleanly on startup!
```

## Technology Stack

| Category         | Technology                        |
|------------------|-----------------------------------|
| Language         | Java 21                           |
| Framework        | Spring Boot 3.4                   |
| Build            | Maven                             |
| Database         | PostgreSQL 16                     |
| Migrations       | Flyway                            |
| Message Broker   | RabbitMQ 3                        |
| Cache            | Redis 7                           |
| API Docs         | Springdoc OpenAPI (Swagger)       |
| Observability    | Micrometer, Spring Actuator       |
| Testing          | JUnit 5, Mockito, Testcontainers  |
| Containerization | Docker, Docker Compose            |

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker and Docker Compose

## Running Locally

### 1. Clone the Repository

```bash
git clone <repository-url>
cd webhook-reconciliation-engine
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env if you want to change defaults
```

### 3. Start Infrastructure

```bash
docker compose up -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

### 4. Build and Start the Application

```bash
mvn clean package -DskipTests
mvn spring-boot:run
```

Or run the packaged JAR:

```bash
java -jar target/webhook-reconciliation-engine-0.1.0-SNAPSHOT.jar
```

### 5. Access Health Endpoint

```
http://localhost:8080/actuator/health
```

### 6. Access Swagger UI

```
http://localhost:8080/swagger-ui.html
```

### 7. Access RabbitMQ Management

```
http://localhost:15672 (guest/guest)
```

### 8. Stop the System

```bash
# Stop the Spring Boot application (Ctrl+C)
# Stop infrastructure
docker compose down

# To also remove data volumes:
docker compose down -v
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Spring Boot Application                │
│                                                          │
│  ┌────────────┐  ┌──────────┐  ┌────────────────────┐   │
│  │ Controllers │──│ Services │──│   Repositories     │   │
│  └────────────┘  └──────────┘  └────────────────────┘   │
│         │              │               │                 │
│         │         ┌────┴────┐          │                 │
│         │         │Messaging│          │                 │
│         │         └────┬────┘          │                 │
└─────────┼──────────────┼───────────────┼─────────────────┘
          │              │               │
   ┌──────▼──────┐ ┌────▼─────┐  ┌──────▼──────┐
   │    Redis    │ │ RabbitMQ │  │ PostgreSQL  │
   │   (Cache)   │ │ (Broker) │  │ (Database)  │
   └─────────────┘ └──────────┘  └─────────────┘
```

## Package Structure

```
src/main/java/com/example/webhookengine/
├── WebhookReconciliationEngineApplication.java
├── controller/       # REST API endpoints
├── service/          # Business logic
├── repository/       # Data access
├── domain/           # Domain entities
├── messaging/        # RabbitMQ integration
├── reconciliation/   # Reconciliation logic
├── security/         # Authentication & validation
├── exception/        # Error handling
├── config/           # Configuration classes
└── common/           # Shared utilities
```

## Future Phases

The following phases are planned for subsequent iterations:

1. **Phase 9** — Security Hardening & Cryptographic Webhook Signatures
2. **Phase 10** — Observability, Metrics & Dashboards
3. **Phase 11** — Performance & Production Readiness

## License

TBD
