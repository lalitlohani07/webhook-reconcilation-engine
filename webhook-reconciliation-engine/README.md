# Webhook Reconciliation Engine

## Description

The Webhook Reconciliation Engine is a reliable, production-grade webhook processing platform designed to receive, validate, persist, and process webhook events from external services.

The system guarantees idempotent processing of webhook events, uses asynchronous message-driven processing with RabbitMQ, and provides automated reconciliation between internal state and external sources of truth. Failed events are retried with exponential backoff and moved to a Dead Letter Queue after permanent failures, with the ability to replay them.

The platform is built for observability, providing health checks, metrics, and an administrative dashboard for monitoring and managing webhook processing.

## Current Status

> **Phase 5 — Retry Handling + Exponential Backoff (Complete)**
>
> Persistent, database-backed retry handling with exponential backoff for failed webhook processing. When an asynchronous worker fails, the execution attempt is recorded in `webhook_attempts`, `attemptCount` is incremented, and if within `max-attempts`, the event status transitions to `RETRY_PENDING` with a calculated `nextRetryAt` timestamp. A dedicated background `WebhookRetryScheduler` polls due retries (`nextRetryAt <= now()`), claims them atomically via conditional updates, and re-enqueues them using the existing Transactional Outbox pattern. Upon exceeding maximum attempts, events transition to `FAILED`.

## Architecture & Messaging Pipeline

### Transactional Outbox & Retry Pipeline
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
           └── WebhookEvent -> FAILED (retries exhausted)

[Retry Scheduler Worker]
WebhookRetryScheduler (@Scheduled polling)
   ↓ Query due events (status = RETRY_PENDING, nextRetryAt <= now)
   ↓ Atomic claim (UPDATE status RETRY_PENDING -> RECEIVED)
   └── Write new OutboxEvent (status: PENDING) in the same PostgreSQL transaction
         ↓
       OutboxPublisher dispatches to RabbitMQ for re-execution
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
`max-attempts` represents the maximum number of processing executions, including the initial execution.
With default `max-attempts: 5`:
- **Attempt 1 fails**: $d_1 = 1000 \times 2^0 = 1000\text{ms}$ (1s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 2 fails**: $d_2 = 1000 \times 2^1 = 2000\text{ms}$ (2s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 3 fails**: $d_3 = 1000 \times 2^2 = 4000\text{ms}$ (4s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 4 fails**: $d_4 = 1000 \times 2^3 = 8000\text{ms}$ (8s) $\rightarrow$ `RETRY_PENDING`
- **Attempt 5 fails**: Maximum attempts reached (5 executions completed) $\rightarrow$ `FAILED`

### Event Lifecycle & Transitions
1. **Initial Acceptance**: `RECEIVED` / `PENDING` (Outbox)
2. **First Execution**: `PROCESSING` $\rightarrow$ `COMPLETED` (Success) OR `RETRY_PENDING` (Retryable failure)
3. **Delayed Re-execution**: `RETRY_PENDING` $\rightarrow$ Claimed by scheduler as `RECEIVED` $\rightarrow$ `PROCESSING`
4. **Exhausted Failures**: `PROCESSING` $\rightarrow$ `FAILED` (when attempt count equals `max-attempts`)

### Publication vs. Processing Failure Separation
- **Publication Failure**: RabbitMQ is unavailable when `OutboxPublisher` attempts delivery. The `OutboxEvent` remains `PENDING` with its `retryCount` incremented. The `WebhookEvent` attempt count is **not** incremented because business processing has not occurred.
- **Processing Failure**: RabbitMQ delivered the message successfully, but `WebhookEventProcessor` threw an exception. A new failed `WebhookAttempt` is recorded, the event's `attemptCount` is incremented, and `WebhookEvent` transitions to `RETRY_PENDING` or `FAILED`. The outbox publication record remains marked as `PUBLISHED`.

### Duplicate Message & Concurrency Protection
- **COMPLETED Events**: If a duplicate message arrives for an already `COMPLETED` event, the worker detects this via database query and skips execution without re-processing.
- **Active PROCESSING Events**: If duplicate concurrent messages arrive, the status check prevents redundant execution.
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

The following phases are planned but **not yet implemented**:

1. **Phase 1** — Domain Model & Database Schema
2. **Phase 2** — Webhook Reception & Validation
3. **Phase 3** — Idempotent Processing
4. **Phase 4** — Asynchronous Processing with RabbitMQ
5. **Phase 5** — Retry & Dead Letter Queue
6. **Phase 6** — Reconciliation Engine
7. **Phase 7** — Observability & Metrics
8. **Phase 8** — Administrative Dashboard
9. **Phase 9** — Security Hardening
10. **Phase 10** — Performance & Production Readiness

## License

TBD
