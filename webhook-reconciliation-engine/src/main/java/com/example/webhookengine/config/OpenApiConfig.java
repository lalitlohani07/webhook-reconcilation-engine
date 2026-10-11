package com.example.webhookengine.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    public static final String BASIC_AUTH_SCHEME = "basicAuth";

    @Bean
    public OpenAPI webhookEngineOpenAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("Webhook Reconciliation Engine API")
                .description("Production-grade Webhook Reconciliation Engine providing transactional ingestion, retry backoff, dead-letter replay, role-based access control, and Redis rate limiting.")
                .version("v1.0.0")
                .contact(new Contact().name("Webhook Engine Team")))
            .components(new Components()
                .addSecuritySchemes(BASIC_AUTH_SCHEME,
                    new SecurityScheme()
                        .name(BASIC_AUTH_SCHEME)
                        .type(SecurityScheme.Type.HTTP)
                        .scheme("basic")
                        .description("HTTP Basic authentication for operational endpoints. Supported roles: ADMIN, OPERATOR, VIEWER.")));
    }
}
