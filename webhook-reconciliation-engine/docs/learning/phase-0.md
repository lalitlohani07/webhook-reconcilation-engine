# Phase 0 — Learning Guide

This document lists the key concepts and technologies used in Phase 0. Each section includes what to understand and why it matters for this project.

---

## 1. Spring Boot Auto-Configuration

- [ ] How `@SpringBootApplication` triggers component scanning and auto-configuration
- [ ] How Spring Boot detects classpath dependencies and configures beans automatically
- [ ] How `application.yml` properties influence auto-configuration
- [ ] The difference between `@Configuration` and auto-configuration

**Key classes:** `@SpringBootApplication`, `@EnableAutoConfiguration`, `@ConditionalOnClass`

---

## 2. Dependency Injection

- [ ] Constructor injection vs field injection (and why constructor injection is preferred)
- [ ] How Spring's IoC container manages bean lifecycle
- [ ] Component scanning and stereotype annotations: `@Component`, `@Service`, `@Repository`, `@Controller`

---

## 3. Maven

- [ ] `pom.xml` structure: `<parent>`, `<dependencies>`, `<build>`, `<properties>`
- [ ] Dependency scopes: `compile`, `runtime`, `test`
- [ ] BOM (Bill of Materials) for version management
- [ ] Maven lifecycle: `clean`, `compile`, `test`, `package`
- [ ] The Spring Boot Maven plugin and executable JARs

---

## 4. PostgreSQL

- [ ] Relational database fundamentals: tables, indexes, constraints
- [ ] ACID properties and why they matter for webhook persistence
- [ ] JDBC driver and connection strings (`jdbc:postgresql://host:port/db`)
- [ ] JSONB data type for semi-structured data

---

## 5. Connection Pools (HikariCP)

- [ ] Why connection pooling is necessary (connection creation is expensive)
- [ ] HikariCP configuration: `maximum-pool-size`, `minimum-idle`, `connection-timeout`
- [ ] How Spring Boot auto-configures HikariCP when a DataSource is present
- [ ] Monitoring pool metrics via Actuator

---

## 6. Flyway

- [ ] Version-controlled database migrations
- [ ] Migration file naming: `V1__description.sql`
- [ ] Migration locations: `classpath:db/migration`
- [ ] The `flyway_schema_history` table
- [ ] `baseline-on-migrate` for existing databases

---

## 7. Docker

- [ ] Container vs virtual machine concepts
- [ ] Dockerfile instructions: `FROM`, `COPY`, `RUN`, `ENTRYPOINT`, `EXPOSE`
- [ ] Multi-stage builds for smaller production images
- [ ] Image layers and caching
- [ ] Running containers: `docker run`, port mapping, volume mounts

---

## 8. Docker Compose

- [ ] `docker-compose.yml` structure: services, ports, volumes, environment variables
- [ ] Service health checks
- [ ] Named volumes for data persistence
- [ ] `docker compose up`, `docker compose down`, `docker compose ps`
- [ ] Environment variable substitution with `.env` files

---

## 9. RabbitMQ Basics

- [ ] AMQP protocol fundamentals: exchanges, queues, bindings, routing keys
- [ ] Message acknowledgment and rejection
- [ ] Dead letter exchanges (DLX) and dead letter queues (DLQ)
- [ ] The RabbitMQ management UI
- [ ] Spring AMQP's `RabbitTemplate` and `@RabbitListener`

---

## 10. Redis Basics

- [ ] Key-value data model
- [ ] Common commands: `SET`, `GET`, `DEL`, `EXPIRE`, `SET NX`
- [ ] TTL (Time To Live) for key expiration
- [ ] Persistence modes: RDB snapshots vs AOF (Append Only File)
- [ ] Spring Data Redis and `RedisTemplate`

---

## 11. Spring Boot Actuator

- [ ] Actuator endpoints: `/actuator/health`, `/actuator/info`, `/actuator/metrics`
- [ ] Health indicators for databases, message brokers, and caches
- [ ] Endpoint exposure and security configuration
- [ ] `show-details: always` vs `show-details: when-authorized`

---

## 12. Health Checks

- [ ] The purpose of health checks in production systems
- [ ] Liveness vs readiness probes
- [ ] How Spring Boot auto-configures health indicators for DataSource, RabbitMQ, Redis
- [ ] Custom health indicators (for future phases)

---

## 13. Environment Configuration

- [ ] Externalized configuration with `application.yml`
- [ ] Environment variable binding: `${VAR_NAME:default}`
- [ ] Profile-specific configuration (`application-dev.yml`, `application-prod.yml`)
- [ ] Why secrets should not be committed to version control
- [ ] `.env` files for local development

---

## 14. Graceful Shutdown

- [ ] What happens during an immediate shutdown (in-flight requests are killed)
- [ ] `server.shutdown=graceful` and `spring.lifecycle.timeout-per-shutdown-phase`
- [ ] How Spring Boot drains in-flight requests before stopping
- [ ] Why graceful shutdown matters when processing webhooks (data integrity)

---

## 15. Testcontainers

- [ ] Running real infrastructure in tests via Docker containers
- [ ] `@Testcontainers` and `@Container` annotations
- [ ] `@DynamicPropertySource` for wiring container connection details into Spring
- [ ] Why integration tests against real databases catch bugs that mock-based tests miss
- [ ] Container lifecycle: started before tests, stopped after tests

---

## Recommended Study Order

1. Maven and project structure
2. Spring Boot auto-configuration and dependency injection
3. Docker and Docker Compose
4. PostgreSQL and HikariCP
5. Flyway migrations
6. Spring Boot Actuator and health checks
7. Environment configuration and secrets management
8. RabbitMQ basics
9. Redis basics
10. Testcontainers
11. Graceful shutdown
