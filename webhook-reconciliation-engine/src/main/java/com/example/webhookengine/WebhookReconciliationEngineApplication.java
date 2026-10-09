package com.example.webhookengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class WebhookReconciliationEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebhookReconciliationEngineApplication.class, args);
    }
}
