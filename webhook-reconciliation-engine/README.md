# Webhook Reconciliation Engine

A resilient, event-driven Java backend platform engineered to solve the hardest problems in webhook processing: distributed dual-writes, out-of-order deliveries, duplicate processing, retry storms, and state divergence from external sources of truth.

---

## 1. Problem Statement & Architecture Goals

In distributed service architectures, receiving webhooks from third-party payment gateways, billing providers, and external SaaS platforms (such as Stripe, Shopify, or GitHub) is fraught with reliability hazards:

- **The Dual-Write Problem**: Writing an event to a database and immediately publishing it to a message broker is not atomic. If the database commit succeeds but the network fails before publishing, the event is lost. If publishing succeeds but the database transaction aborts, the worker processes a phantom event.
- **At-Least-Once Delivery & Duplicates**: Distributed brokers guarantee at-least-once delivery, not exactly-once delivery. Network latency, retries, and dropped acknowledgements frequently cause duplicate deliveries of the same message.
- **Cascading Retry Storms**: Naive retry implementations repeatedly invoke failing downstream dependencies immediately, magnifying upstream outages and causing connection pool exhaustion.
- **Dead-Letter Invisibility**: When events exceed retry budgets, they are typically routed to an unmonitored Dead Letter Queue (DLQ) where failures remain invisible and difficult to remediate safely.
- **State Drift**: Downstream consumers can succeed locally while upstream providers fail, or vice versa, causing silent financial and transactional divergence.

### How This Platform Solves Them

The **Webhook Reconciliation Engine** implements battle-tested distributed systems patterns to address each hazard:
1. **Transactional Outbox Pattern**: Ingested webhooks and outbox publication records are committed atomically to PostgreSQL within the same ACID transaction. An asynchronous poller with RabbitMQ correlated publisher confirms dispatches events reliably.
2. **Authoritative Idempotency & Pessimistic Locking**: Ingestion enforces uniqueness via database composite keys `UNIQUE(source, event_id)`. Workers claim events using pessimistic write locks (`SELECT ... FOR UPDATE`), ensuring that duplicated or concurrent message deliveries do not trigger duplicate domain processing.
3. **Database-Backed Exponential Backoff**: Failures are scheduled with exponential backoff ($d_n = \min(d_0 \cdot m^{n-1}, d_{\max})$) and persisted durably in PostgreSQL. Message workers remain unblocked while a dedicated scheduler re-enqueues due retries through the outbox.
4. **Dead-Letter Management & Replay Audit**: Exhausted events transition to `DEAD_LETTER` with zero automatic retries. Operators inspect chronological attempt histories and trigger authorized replays via REST APIs, creating immutable audit logs in `webhook_replays`.
5. **Atomic Redis Rate Limiting**: Protects ingestion from volumetric floods using atomic Redis Lua scripts (`INCR` + `EXPIRE`). Throttled requests reject with HTTP 429 without initiating database transactions.
6. **Automated Source-of-Truth Reconciliation**: An automated engine periodically inspects terminal events against an external source-of-truth abstraction (`SourceOfTruthClient`), categorizing records into `MATCHED`, `CORRECTED`, `MISMATCH`, and `FAILED` with JSON comparison snapshots.

---

## 2. Technology Stack & Architectural Roles

| Technology | Version | Architectural Role | Rationale |
|---|---|---|---|
| **Java** | 21 (LTS) | Core Programming Language | Strong typing, records for immutable DTOs, modern concurrency primitives. |
| **Spring Boot** | 3.4.x | Backend Application Framework | Production-ready Dependency Injection, Spring MVC, Spring Data JPA, Spring Security, and AMQP. |
| **PostgreSQL** | 16 | Relational ACID Database | Authoritative state store, native JSONB support for webhook payloads, pessimistic row locking (`FOR UPDATE`). |
| **Flyway** | 10.x | Database Schema Migrations | Version-controlled, immutable schema migrations (`V1` to `V4`) paired with `ddl-auto: validate` to guarantee database schema integrity. |
| **RabbitMQ** | 3.13 | Asynchronous Message Broker | Decouples ingestion from business processing; correlated publisher confirms and durable queues. |
| **Redis** | 7 | Distributed In-Memory Cache | Distributed fixed-window rate limiter evaluated atomically via single-threaded Redis Lua scripts. |
| **Spring Security** | 6.4.x | Authentication & Authorization | Stateless HTTP Basic authentication, BCrypt password hashing, and role-based access control (`ADMIN`, `OPERATOR`, `VIEWER`). |
| **Spring Actuator** | 3.4.x | Observability & Health Checks | Liveness and readiness health checks across PostgreSQL, RabbitMQ, and Redis. |
| **Springdoc OpenAPI** | 2.7.x | API Documentation | Interactive OpenAPI 3 / Swagger documentation UI. |
| **Testcontainers** | 1.20.x | Integration Testing | Spin up real PostgreSQL and RabbitMQ containers for automated failure and concurrency integration tests. |

---

## 3. Architecture & Processing Pipelines

### High-Level System Overview
```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       SPRING BOOT APPLICATION                                          │
│                                                                                                        │
│  ┌────────────────────────┐         ┌────────────────────────┐         ┌────────────────────────────┐  │
│  │ WebhookController      │         │ DeadLetterController   │         │ ReconciliationController   │  │
│  │ (Public + Rate-Limited)│         │ (Protected RBAC)       │         │ (Protected RBAC)           │  │
│  └───────────┬────────────┘         └───────────┬────────────┘         └─────────────┬──────────────┘  │
│              │                                  │                                    │                 │
│              ▼                                  ▼                                    ▼                 │
│  ┌────────────────────────┐         ┌────────────────────────┐         ┌────────────────────────────┐  │
│  │ WebhookIngestionService│         │ DeadLetterService      │         │ ReconciliationService      │  │
│  └───────────┬────────────┘         └───────────┬────────────┘         └─────────────┬──────────────┘  │
│              │                                  │                                    │                 │
│              │  ┌───────────────────────────────┴─────────────────────────────────┐  │                 │
│              ▼  ▼                                                                 ▼  ▼                 │
│         PostgreSQL (ACID State, Outbox, Attempts, Replays, Reconciliations)                            │
│              │                                                                                         │
│              ▼                                                                                         │
│  ┌────────────────────────┐                     ┌────────────────────────┐                             │
│  │ OutboxPublisher        │ ──[Publisher Confirm]─▶│ RabbitMQ Broker        │                             │
│  │ (@Scheduled Poller)    │                     │ (webhook.events)       │                             │
│  └────────────────────────┘                     └───────────┬────────────┘                             │
│                                                             │                                          │
│                                                             ▼                                          │
│  ┌────────────────────────┐                     ┌────────────────────────┐                             │
│  │ WebhookRetryScheduler  │                     │ WebhookConsumer        │                             │
│  │ (@Scheduled Poller)    │                     │ (Idempotent Worker)    │                             │
│  └────────────────────────┘                     └────────────────────────┘                             │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Complete 10-Step End-to-End Lifecycle
```text
[1. Ingest]        HTTP Client ──▶ POST /api/v1/webhooks
                      │
                      ▼
[2. DB Tx]         PostgreSQL Transaction:
                      ├── Persist WebhookEvent (status: RECEIVED)
                      └── Persist OutboxEvent (status: PENDING)
                      │
                      ▼
[3. Outbox Pub]    OutboxPublisher poller ──▶ RabbitMQ exchange (webhook.events)
                      ├── ACK received ──▶ OutboxEvent marked PUBLISHED
                      └── NACK / Timeout ──▶ OutboxEvent remains PENDING (retryCount++)
                      │
                      ▼
[4. Worker Exec]   WebhookProcessingConsumer ──▶ WebhookProcessingService
                      ├── Row lock: SELECT ... FOR UPDATE (prevents concurrent worker races)
                      ├── If status != RECEIVED ──▶ Acknowledge & Skip duplicate delivery
                      └── Status -> PROCESSING, insert WebhookAttempt (attempt: 1)
                      │
                      ├─── [On Success] ──▶ WebhookEvent -> COMPLETED, WebhookAttempt -> COMPLETED
                      │
                      └─── [On Failure]
                              │
[5. Backoff]                  ├── If attempt < maxAttempts:
                              │     └── WebhookEvent -> RETRY_PENDING, nextRetryAt = now + delay
                              │         WebhookRetryScheduler claims due event -> enqueues OutboxEvent
                              │
[6. DLQ]                      └── If attempt >= maxAttempts:
                                    └── WebhookEvent -> DEAD_LETTER, nextRetryAt = null
                                          │
                                          ▼
[7. DLQ Inspect]   Operator Client ──▶ GET /api/v1/dead-letters/{id} (Auth: OPERATOR/ADMIN)
                      │
                      ▼
[8. Replay]        Operator Client ──▶ POST /api/v1/dead-letters/{id}/replay {"reason": "..."}
                      │
                      ▼
[9. Replay Audit]  PostgreSQL Transaction:
                      ├── Atomic conditional claim: UPDATE WHERE status = 'DEAD_LETTER' -> RECEIVED
                      ├── Insert audit record into webhook_replays
                      └── Insert OutboxEvent (status: PENDING) -> Published to RabbitMQ for Attempt N+1
                      │
                      ▼
[10. Reconcile]    Operator Client ──▶ POST /api/v1/reconciliation/runs
                      └── Compares terminal events against SourceOfTruthClient -> Persists ReconciliationRecord
```

---

## 4. Completed Project Phases

- **Phase 0 — Infrastructure Skeleton**: Multi-container Docker Compose setup (PostgreSQL, RabbitMQ, Redis), Flyway configuration, Spring Actuator health probes, and Testcontainers baseline.
- **Phase 1 — Domain Modeling & Flyway Schema**: Relational entities (`WebhookEvent`, `WebhookAttempt`, `OutboxEvent`, `ReconciliationRecord`), UTC timestamp mappings, JSONB payload handling, and initial migration `V1`.
- **Phase 2 — Ingestion API & Database Idempotency**: `POST /api/v1/webhooks` endpoint, strict Jakarta Bean Validation, composite unique key deduplication `UNIQUE(source, event_id)`, and concurrent race fallback.
- **Phase 3 — Asynchronous Message Worker**: RabbitMQ direct exchange bindings, `WebhookEventPublisher`, `WebhookProcessingConsumer`, worker row locking, and deterministic attempt tracking.
- **Phase 4 — Transactional Outbox Pattern**: Atomic dual-write elimination, scheduled outbox polling, RabbitMQ correlated publisher confirms with timeout fallbacks, and crash resilience.
- **Phase 5 — Retry Scheduling & Exponential Backoff**: Database-backed exponential backoff policy ($d_n = \min(d_0 \cdot m^{n-1}, d_{\max})$), `WebhookRetryScheduler`, atomic status transition, and transport vs. business failure separation.
- **Phase 6 — Dead-Letter Management & Controlled Replay**: `DEAD_LETTER` state machine, paginated inspection API, atomic replay claiming via conditional SQL updates, and immutable audit logs in `webhook_replays`.
- **Phase 7 — Role-Based Security & Distributed Rate Limiting**: Stateless HTTP Basic authentication, BCrypt user persistence, RBAC enforcement (`ADMIN`, `OPERATOR`, `VIEWER`), and atomic Redis Lua script rate limiter with fail-closed outage protection.
- **Phase 8 — Reconciliation Engine**: Decoupled `SourceOfTruthClient` abstraction with simulated provider, bounded batch execution, per-event isolated transactions (`REQUIRES_NEW`), and audit classification (`MATCHED`, `CORRECTED`, `MISMATCH`, `FAILED`).
- **Phase 9 — End-to-End Reliability & Failure Testing**: Verified complete 10-step lifecycle and 14 failure/concurrency scenarios; resolved transactional rollback loss via `noRollbackFor = Exception.class`, eliminated worker races via pessimistic row-level locking (`findByIdForUpdate`), and addressed Spring proxy self-invocation bypasses via `TransactionTemplate`.

---

## 5. Prerequisites & Local Setup

### System Prerequisites
- **Java 21 JDK** (Eclipse Temurin, OpenJDK, or Amazon Corretto)
- **Maven 3.9+**
- **Docker & Docker Compose** (v2+)

### Step 1: Clone the Repository
```bash
git clone https://github.com/your-username/webhook-reconciliation-engine.git
cd webhook-reconciliation-engine/webhook-reconciliation-engine
```

### Step 2: Configure Environment Variables
Copy the environment template:
```bash
cp .env.example .env
```

Review and adjust variables in `.env` as needed. The default development values are preconfigured for local Docker Compose services:

| Environment Variable | Default Value | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `webhook_engine` | Database name |
| `DB_USERNAME` | `webhook_user` | Database username |
| `DB_PASSWORD` | `webhook_pass` | Database password |
| `RABBITMQ_HOST` | `localhost` | RabbitMQ host |
| `RABBITMQ_PORT` | `5672` | RabbitMQ AMQP port |
| `RABBITMQ_MGMT_PORT` | `15672` | RabbitMQ Management Web UI port |
| `RABBITMQ_USERNAME` | `guest` | RabbitMQ username |
| `RABBITMQ_PASSWORD` | `guest` | RabbitMQ password |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `APP_SECURITY_BOOTSTRAP_USERNAME` | `admin` | Initial administrator username |
| `APP_SECURITY_BOOTSTRAP_PASSWORD` | `your_secure_admin_password_here` | Initial administrator password |
| `RATE_LIMIT_CAPACITY` | `60` | Max webhook ingestion requests per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Rate limit window duration in seconds |
| `RATE_LIMIT_FAIL_CLOSED` | `true` | Fail-closed policy on Redis outages |

> [!CAUTION]
> Never commit `.env` files containing production passwords or sensitive connection strings. The repository `.gitignore` automatically excludes `.env`.

### Step 3: Start Infrastructure with Docker Compose
Start PostgreSQL, RabbitMQ, and Redis:
```bash
docker compose up -d
```

Verify that all three services pass their container healthchecks:
```bash
docker compose ps
```
*Expected output: `wre-postgres (healthy)`, `wre-rabbitmq (healthy)`, `wre-redis (healthy)`.*

### Step 4: Build and Run the Application
Compile the code and package the executable JAR:
```bash
mvn clean package -DskipTests
```

Run the application:
```bash
mvn spring-boot:run
```
Or run the packaged JAR directly:
```bash
java -jar target/webhook-reconciliation-engine-0.1.0-SNAPSHOT.jar
```

The application will start on port `8080`.

---

## 6. Database Schema & Flyway Migrations

Database migrations are managed strictly through Flyway. Spring Boot is configured with `spring.jpa.hibernate.ddl-auto: validate`, ensuring Hibernate verifies the schema at startup but never modifies tables in production.

Migrations execute automatically in alphabetical/version order on application boot:

- **`V1__create_webhook_domain_tables.sql`**:
  - `webhook_events`: Primary event store with composite constraint `UNIQUE(source, event_id)` and JSONB `payload`.
  - `webhook_attempts`: Delivery history with composite constraint `UNIQUE(webhook_event_id, attempt_number)`.
  - `outbox_events`: Transactional outbox table storing outgoing AMQP payloads and publication statuses (`PENDING`, `PUBLISHED`, `FAILED`).
  - `reconciliation_records`: Comparison audit records referencing webhook events.
- **`V2__create_replay_audit_and_indexes.sql`**:
  - `webhook_replays`: Operator replay audit log tracking requested timestamp, reason, and previous attempt counts.
  - Index `idx_webhook_events_status_created_at` on `(status, created_at DESC)` for DLQ pagination.
- **`V3__create_security_users_table.sql`**:
  - `security_users`: Operational user accounts with BCrypt password hashes, roles (`ADMIN`, `OPERATOR`, `VIEWER`), and enabled flags.
- **`V4__create_reconciliation_indexes_and_constraints.sql`**:
  - Unique constraint `uk_reconciliation_records_event_id` on `reconciliation_records(webhook_event_id)` to prevent duplicate reconciliation audits.

---

## 7. API Endpoint Reference & RBAC Matrix

All operational endpoints require HTTP Basic authentication. Passwords are encrypted with BCrypt (`BCryptPasswordEncoder`).

| Endpoint | Method | Public | VIEWER | OPERATOR | ADMIN | Description |
|---|---|---|---|---|---|---|
| `/api/v1/webhooks` | `POST` | Allow | Allow | Allow | Allow | Ingest webhook event (subject to Redis rate limiting) |
| `/api/v1/dead-letters` | `GET` | 401 | Allow | Allow | Allow | Paginated list of dead-letter events |
| `/api/v1/dead-letters/{id}` | `GET` | 401 | Allow | Allow | Allow | Inspect single dead-letter event details & attempts |
| `/api/v1/dead-letters/{id}/replay` | `POST` | 401 | 403 | Allow | Allow | Replay dead-letter event through outbox |
| `/api/v1/reconciliation/runs` | `POST` | 401 | 403 | Allow | Allow | Trigger bounded reconciliation batch |
| `/api/v1/reconciliation/records` | `GET` | 401 | Allow | Allow | Allow | Paginated list of reconciliation audit records |
| `/api/v1/reconciliation/records/{id}` | `GET` | 401 | Allow | Allow | Allow | Inspect reconciliation comparison details |
| `/actuator/health` | `GET` | Allow | Allow | Allow | Allow | Infrastructure health status |
| `/swagger-ui.html` | `GET` | Allow | Allow | Allow | Allow | Interactive OpenAPI Swagger UI |

---

## 8. Representative curl Examples & Responses

### 1. Webhook Ingestion (`POST /api/v1/webhooks`)

#### New Event Accepted (`202 Accepted`)
```bash
curl -i -X POST http://localhost:8080/api/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "evt_order_1001",
    "source": "shopify",
    "eventType": "order.created",
    "payload": {
      "orderId": "ord_9901",
      "total": 89.50,
      "currency": "USD"
    }
  }'
```
**Response**:
```http
HTTP/1.1 202 Accepted
Content-Type: application/json
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 59

{
  "eventId": "evt_order_1001",
  "source": "shopify",
  "status": "RECEIVED",
  "message": "Webhook accepted for processing"
}
```

#### Duplicate Submission Idempotency (`200 OK`)
Submitting the identical `(source, eventId)` again returns `200 OK` with the existing status without modifying the database or triggering duplicate worker jobs:
```http
HTTP/1.1 200 OK
Content-Type: application/json
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 58

{
  "eventId": "evt_order_1001",
  "source": "shopify",
  "status": "RECEIVED",
  "message": "Webhook event already received"
}
```

#### Rate Limit Exhausted (`429 Too Many Requests`)
When request volume exceeds the configured window capacity:
```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 42
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 0

{
  "status": 429,
  "error": "Too Many Requests",
  "message": "Rate limit exceeded for source: shopify. Please retry after 42 seconds.",
  "timestamp": "2026-10-12T10:00:00Z"
}
```

#### Redis Infrastructure Outage (`503 Service Unavailable`)
If Redis is down in `fail-closed` mode, the engine protects databases from overload:
```http
HTTP/1.1 503 Service Unavailable
Content-Type: application/json
Retry-After: 30

{
  "status": 503,
  "error": "Service Unavailable",
  "message": "Rate limiting service is temporarily unavailable. Please retry later.",
  "timestamp": "2026-10-12T10:00:00Z"
}
```

---

### 2. Inspecting Dead-Letter Events (`GET /api/v1/dead-letters/{id}`)

```bash
curl -i -X GET http://localhost:8080/api/v1/dead-letters/3fa85f64-5717-4562-b3fc-2c963f66afa6 \
  -u "admin:your_secure_admin_password_here"
```
**Response (`200 OK`)**:
```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "eventId": "evt_charge_failed_999",
  "source": "stripe",
  "eventType": "payment_intent.payment_failed",
  "status": "DEAD_LETTER",
  "attemptCount": 5,
  "lastError": "Connection timed out connecting to payment service",
  "createdAt": "2026-10-12T09:00:00Z",
  "updatedAt": "2026-10-12T09:05:00Z",
  "nextRetryAt": null,
  "attempts": [
    {
      "id": "b0f7e4e1-7a6c-4b68-b7c1-15bce6f2db01",
      "attemptNumber": 1,
      "status": "FAILED",
      "startedAt": "2026-10-12T09:00:01Z",
      "completedAt": "2026-10-12T09:00:02Z",
      "errorMessage": "503 Service Unavailable"
    },
    {
      "id": "c1f7e4e1-7a6c-4b68-b7c1-15bce6f2db02",
      "attemptNumber": 2,
      "status": "FAILED",
      "startedAt": "2026-10-12T09:00:03Z",
      "completedAt": "2026-10-12T09:00:04Z",
      "errorMessage": "Connection timed out connecting to payment service"
    }
  ],
  "replays": []
}
```

---

### 3. Replaying a Dead-Letter Event (`POST /api/v1/dead-letters/{id}/replay`)

```bash
curl -i -X POST http://localhost:8080/api/v1/dead-letters/3fa85f64-5717-4562-b3fc-2c963f66afa6/replay \
  -u "admin:your_secure_admin_password_here" \
  -H "Content-Type: application/json" \
  -d '{"reason": "Downstream payment gateway connectivity restored"}'
```
**Response (`202 Accepted`)**:
```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "eventId": "evt_charge_failed_999",
  "source": "stripe",
  "status": "RECEIVED",
  "message": "Webhook event replay accepted and enqueued for processing"
}
```

#### Replay Error Cases
- **Unauthorized (`401 Unauthorized`)**: Missing or invalid credentials.
- **Forbidden (`403 Forbidden`)**: Attempted by an authenticated account with the `VIEWER` role.
- **Conflict (`409 Conflict`)**: Event is not in `DEAD_LETTER` status, or another concurrent operator won the replay race.

---

### 4. Reconciliation Runs & Inspection

#### Trigger a Reconciliation Batch (`POST /api/v1/reconciliation/runs`)
```bash
curl -i -X POST http://localhost:8080/api/v1/reconciliation/runs \
  -u "admin:your_secure_admin_password_here" \
  -H "Content-Type: application/json" \
  -d '{
    "batchSize": 50,
    "source": "stripe",
    "includeAlreadyReconciled": false
  }'
```
**Response (`200 OK`)**:
```json
{
  "runId": "a98dfb01-5768-4541-bbf2-d33a6f1b34e2",
  "totalEligible": 2,
  "processedCount": 2,
  "matchedCount": 1,
  "mismatchCount": 1,
  "correctedCount": 0,
  "failedCount": 0,
  "records": [
    {
      "id": "7b0d77f2-1d59-4ee4-9441-26c7d1e8ef90",
      "webhookEventId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
      "eventId": "evt_payment_001",
      "source": "stripe",
      "eventType": "payment_intent.succeeded",
      "result": "MATCHED",
      "discrepancyReason": null,
      "reconciledAt": "2026-10-12T10:15:00Z"
    }
  ]
}
```

#### Inspect Reconciliation Record Detail (`GET /api/v1/reconciliation/records/{id}`)
```bash
curl -i -X GET http://localhost:8080/api/v1/reconciliation/records/7b0d77f2-1d59-4ee4-9441-26c7d1e8ef90 \
  -u "admin:your_secure_admin_password_here"
```
**Response (`200 OK`)**:
```json
{
  "id": "7b0d77f2-1d59-4ee4-9441-26c7d1e8ef90",
  "webhookEventId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "eventId": "evt_payment_002",
  "source": "stripe",
  "eventType": "payment_intent.succeeded",
  "eventStatus": "DEAD_LETTER",
  "result": "CORRECTED",
  "discrepancyReason": "External source of truth indicates COMPLETED while local status was DEAD_LETTER",
  "details": {
    "localStatus": "DEAD_LETTER",
    "externalStatus": "COMPLETED",
    "externalEventId": "evt_payment_002",
    "externalSource": "stripe",
    "checkedAt": "2026-10-12T10:15:00Z",
    "note": "Local event remains DEAD_LETTER; external system confirms business completion"
  },
  "reconciledAt": "2026-10-12T10:15:00Z"
}
```

---

## 9. Testing & Reliability Verification

### Test Suite Execution
The automated test suite contains unit tests, Spring WebMvc controller tests, and full data-layer integration tests.

Run the test suite:
```bash
mvn clean test
```

### Current Test Results
```text
[INFO] Results:
[INFO] 
[WARNING] Tests run: 156, Failures: 0, Errors: 0, Skipped: 3
[INFO] 
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  02:21 min
```
- **Total Tests Run**: 156
- **Passed**: 153
- **Failures / Errors**: 0
- **Skipped**: 3 (`DatabaseConnectionIntegrationTest` (1) and `PostgresReliabilityTestcontainersIntegrationTest` (2))

### Distinguishing H2 Compatibility vs. Real PostgreSQL Testing
- **In-Memory Testing**: The 153 passing tests run against an in-memory H2 database operating in PostgreSQL syntax compatibility mode. While this verifies business logic and mapping fidelity, H2 does not replicate PostgreSQL's real multi-version concurrency control (MVCC) or exact row-level locking semantics under parallel transactions.
- **Real Container Testing (`Testcontainers`)**: The project provides [`PostgresReliabilityTestcontainersIntegrationTest`](src/test/java/com/example/webhookengine/integration/PostgresReliabilityTestcontainersIntegrationTest.java) to test against genuine PostgreSQL 16 and RabbitMQ 3.13 Docker containers.
- **Docker Socket Environment Limitation**: In local development or restricted CI environments where the user account lacks read/write permissions to `/var/run/docker.sock`, Testcontainers tests detect Docker unavailability via `@EnabledIf("isDockerAvailable")` and skip gracefully without failing the Maven build.

#### Running Real Testcontainers Tests
When Docker daemon access is available:
```bash
# 1. Ensure user is in the docker group:
sudo usermod -aG docker $USER
newgrp docker

# 2. Run Testcontainers integration tests:
mvn test -Dtest=PostgresReliabilityTestcontainersIntegrationTest
```

---

## 10. Known Limitations

1. **Simulated Source of Truth**: The reconciliation engine integrates with `SimulatedSourceOfTruthClient` for deterministic test verification. Production adapters for concrete third-party APIs (e.g., Stripe API, GitHub REST API) must be implemented behind the `SourceOfTruthClient` interface.
2. **Cryptographic Webhook Signatures**: The ingestion endpoint accepts unauthenticated webhooks and enforces rate limiting, but does not yet verify cryptographic HMAC SHA-256 headers (planned for Phase 11).
3. **Non-Mutating `CORRECTED` Classification**: When reconciliation identifies that an external provider settled a transaction successfully despite a local `DEAD_LETTER` failure, it creates a `CORRECTED` audit record but deliberately preserves the local event status as `DEAD_LETTER` to preserve immutable audit trails.

---

## 11. Project Roadmap

| Phase | Description | Status |
|---|---|---|
| **Phase 0** | Infrastructure Skeleton (PostgreSQL, RabbitMQ, Redis, Docker Compose) | Completed |
| **Phase 1** | Domain Model & Flyway Schema Migrations | Completed |
| **Phase 2** | Webhook Ingestion API, Validation & Idempotency | Completed |
| **Phase 3** | RabbitMQ Asynchronous Processing Pipeline | Completed |
| **Phase 4** | Transactional Outbox Pattern & Publisher Confirms | Completed |
| **Phase 5** | Retry Scheduling & Exponential Backoff Engine | Completed |
| **Phase 6** | Dead-Letter Management, Inspection & Audited Replay | Completed |
| **Phase 7** | Spring Security RBAC & Redis Distributed Rate Limiting | Completed |
| **Phase 8** | Automated Reconciliation Engine against Source of Truth | Completed |
| **Phase 9** | End-to-End Reliability, Concurrency & Failure Resilience | Completed |
| **Phase 10** | Portfolio Readiness, Clean Code & Documentation Polish | Completed |
| **Phase 11** | Cryptographic Webhook Signatures (HMAC SHA-256 per provider) | Planned |
| **Phase 12** | Observability, Metrics & Dashboards (Prometheus & Grafana) | Planned |
| **Phase 13** | Performance Optimization & Multi-Region Clustering | Planned |

---

## 12. License

This project is licensed under the MIT License — see the LICENSE file for details.
