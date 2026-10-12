# Webhook Reconciliation Engine

[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot 3.4](https://img.shields.io/badge/Spring%20Boot-3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![RabbitMQ 3.13](https://img.shields.io/badge/RabbitMQ-3.13-orange.svg)](https://www.rabbitmq.com/)
[![Redis 7](https://img.shields.io/badge/Redis-7-red.svg)](https://redis.io/)
[![Build & Tests](https://img.shields.io/badge/Tests-153%20Passed-success.svg)](https://github.com/)

A resilient, event-driven Java backend platform engineered to solve distributed webhook ingestion, delivery, and consistency challenges. It implements the **Transactional Outbox Pattern**, **Idempotent Asynchronous Worker Processing**, **Database-Backed Exponential Backoff Retries**, a **Dead-Letter State Machine with Audited Replays**, **Atomic Redis Rate Limiting**, and **Automated Source-of-Truth Reconciliation**.

---

## Repository Structure

```
.
├── .vscode/                                 # Editor configuration
└── webhook-reconciliation-engine/           # Main Spring Boot backend project
    ├── src/main/java/                       # Application source code
    │   └── com/example/webhookengine/
    │       ├── config/                      # RabbitMQ, OpenAPI, Retry configuration
    │       ├── controller/                  # REST API endpoints (Webhooks, DLQ, Reconciliation)
    │       ├── domain/                      # JPA Entities (WebhookEvent, OutboxEvent, Attempts, etc.)
    │       ├── messaging/                   # RabbitMQ publishers, consumers, outbox poller
    │       ├── ratelimit/                   # Atomic Redis Lua script rate limiter
    │       ├── reconciliation/              # Source-of-truth reconciliation engine
    │       ├── repository/                  # Spring Data JPA repositories with row locking
    │       ├── security/                    # Spring Security HTTP Basic RBAC & BCrypt users
    │       └── service/                     # Core business logic, retry policy, schedulers
    ├── src/main/resources/
    │   ├── db/migration/                    # Versioned Flyway migrations (V1 to V4)
    │   └── application.yml                  # Central configuration
    ├── src/test/java/                       # Unit, integration, and failure scenario tests
    ├── docker-compose.yml                   # PostgreSQL, RabbitMQ, and Redis containers
    ├── Dockerfile                           # Multi-stage container build
    ├── pom.xml                              # Maven build descriptor
    ├── ARCHITECTURE.md                      # Detailed technical architecture & decision rationale
    └── README.md                            # Comprehensive setup, API reference & curl examples
```

---

## Key Highlights

- **Dual-Write Elimination**: Combines database persistence and message enqueuing in a single ACID transaction via the Transactional Outbox pattern.
- **Strict Idempotency**: Guarantees zero duplicate processing using PostgreSQL composite unique constraints and row-level pessimistic locking (`SELECT ... FOR UPDATE`).
- **Resilient Retry Policy**: Exponential backoff with maximum caps ($d_n = \min(d_0 \cdot m^{n-1}, d_{\max})$) scheduled via PostgreSQL without tying up worker threads.
- **Dead-Letter Auditing & Replay**: Halts retry storms when events exhaust retries, exposing protected REST endpoints (`ADMIN`, `OPERATOR`) for inspection and atomic replay.
- **Distributed Ingestion Protection**: High-throughput fixed-window rate limiter executed atomically via single-threaded Redis Lua scripts (`INCR` + `EXPIRE`). Fails closed on Redis outages without touching the database.
- **Source-of-Truth Reconciliation**: Periodically compares terminal local events against an independent external source-of-truth interface (`SourceOfTruthClient`), persisting structured JSON comparison audits.
- **Exhaustive Reliability Testing**: 156 integration and unit tests validating 14 failure and concurrency scenarios, including broker outages, worker crashes, application restarts, and concurrent requests.

---

## Quick Start

```bash
# 1. Navigate to the project directory
cd webhook-reconciliation-engine

# 2. Configure environment
cp .env.example .env

# 3. Start PostgreSQL, RabbitMQ, and Redis
docker compose up -d

# 4. Build and execute tests
mvn clean test

# 5. Start the Spring Boot application
mvn spring-boot:run
```

Access OpenAPI Swagger UI at: `http://localhost:8080/swagger-ui.html`  
Access Spring Actuator Health at: `http://localhost:8080/actuator/health`

For complete documentation, API reference, and architecture deep dives, see:
- [Detailed Project README](webhook-reconciliation-engine/README.md)
- [Architecture & Design Rationale](webhook-reconciliation-engine/ARCHITECTURE.md)
