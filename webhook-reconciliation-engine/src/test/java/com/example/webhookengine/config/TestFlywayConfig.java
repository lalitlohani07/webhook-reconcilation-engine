package com.example.webhookengine.config;

import org.flywaydb.core.api.callback.BaseCallback;
import org.flywaydb.core.api.callback.Context;
import org.flywaydb.core.api.callback.Event;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Statement;

@Configuration
public class TestFlywayConfig {

    @Bean
    public org.flywaydb.core.api.callback.Callback h2JsonbCompatibilityCallback() {
        return new BaseCallback() {
            @Override
            public void handle(Event event, Context context) {
                if (event == Event.BEFORE_MIGRATE) {
                    try {
                        String databaseProductName = context.getConnection().getMetaData().getDatabaseProductName();
                        if ("H2".equalsIgnoreCase(databaseProductName)) {
                            try (Statement stmt = context.getConnection().createStatement()) {
                                stmt.execute("CREATE DOMAIN IF NOT EXISTS JSONB AS JSON");
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to register H2 JSONB alias", e);
                    }
                }
            }

            @Override
            public boolean supports(Event event, Context context) {
                return event == Event.BEFORE_MIGRATE;
            }

            @Override
            public boolean canHandleInTransaction(Event event, Context context) {
                return true;
            }
        };
    }
}
