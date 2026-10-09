package com.example.webhookengine;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import javax.sql.DataSource;

@SpringBootTest(properties = {
    "spring.flyway.enabled=false",
    "spring.jpa.hibernate.ddl-auto=none",
    "spring.jpa.database-platform=org.hibernate.dialect.PostgreSQLDialect",
    "management.health.redis.enabled=false",
    "management.health.rabbit.enabled=false",
    "management.health.db.enabled=false",
    "app.outbox.enabled=false",
    "app.retry.enabled=false"
})
class WebhookReconciliationEngineApplicationTest {

    @MockitoBean
    private DataSource dataSource;

    @MockitoBean
    private ConnectionFactory connectionFactory;

    @MockitoBean
    private org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory redisConnectionFactory;

    @Test
    void contextLoads() {
        // Verifies that the Spring Boot application context can load cleanly
    }
}
