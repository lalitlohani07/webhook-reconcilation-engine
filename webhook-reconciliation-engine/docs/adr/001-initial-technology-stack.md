# ADR-001: Initial Technology Stack

## Status

Accepted

## Date

2026-10-08

## Context

We are building a Webhook Reconciliation Engine — a backend system that must reliably receive, persist, and process webhook events, retry failures, and reconcile internal state with external systems. We need to select the initial technology stack that supports these requirements.

## Decision

### Java 21

- Long-term support (LTS) release with stable, well-understood runtime behavior
- Virtual threads (Project Loom) available for future high-concurrency scenarios
- Pattern matching, records, and sealed classes improve domain modeling
- Extensive ecosystem for enterprise backend systems

### Spring Boot 3.4

- De facto standard for Java backend applications with production-ready defaults
- Spring AMQP provides mature RabbitMQ integration with built-in retry and DLQ support
- Spring Data JDBC offers a simpler persistence model than JPA/Hibernate for our use case
- Spring Actuator provides health checks and metrics out of the box
- Auto-configuration reduces boilerplate while remaining transparent

### PostgreSQL 16

- ACID transactions are essential — webhook events must not be lost or duplicated
- JSONB support for storing heterogeneous webhook payloads without rigid schema
- Proven reliability at scale for transactional workloads
- Excellent tooling, monitoring, and operational knowledge base
- Free and open source with no licensing constraints

### RabbitMQ 3

- Native acknowledgment model ensures messages are not lost during processing
- Dead letter exchanges (DLX) provide built-in DLQ support without custom code
- Message TTL and per-message expiration support retry with backoff
- Management UI provides operational visibility without building custom dashboards immediately
- Spring AMQP integration is mature and well-documented
- Chose RabbitMQ over Kafka because our use case is task-queue-oriented (process each webhook once) rather than event-stream-oriented (replay event logs)

### Redis 7

- SET NX with TTL provides atomic idempotency key management
- Sub-millisecond latency for checking whether a webhook has already been processed
- Simple key-value model is a natural fit for idempotency checks and caching
- Append-only persistence provides durability suitable for local development and can be tuned for production

### Flyway

- Version-controlled, repeatable database migrations
- Plain SQL migrations are transparent and auditable
- Integrates natively with Spring Boot auto-configuration
- Simpler than Liquibase for SQL-first migration workflows
- Migration history is stored in the database itself, making it self-documenting

### Docker / Docker Compose

- Consistent development environment across machines
- Infrastructure-as-code for local PostgreSQL, RabbitMQ, and Redis
- Multi-stage Dockerfile produces minimal production images
- Docker Compose simplifies multi-service orchestration for development
- Same container images can be used in CI/CD pipelines

### Testcontainers

- Integration tests run against real infrastructure (PostgreSQL, RabbitMQ) instead of mocks
- Eliminates the class of bugs where tests pass against mocks but fail against real services
- JUnit 5 integration manages container lifecycle automatically
- Uses the same Docker images as production, reducing environment drift

## Consequences

- The team must have Docker installed for local development and testing
- Java 21 is required; code should not use features beyond Java 21
- Spring Boot's opinionated defaults are accepted unless they conflict with specific requirements
- PostgreSQL is the only supported database; the system is not database-agnostic
- RabbitMQ is the only supported message broker; switching to Kafka would require architectural changes
