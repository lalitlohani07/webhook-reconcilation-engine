package com.example.webhookengine.service;

import com.example.webhookengine.domain.WebhookEvent;

public record IngestionResult(WebhookEvent event, boolean isNew) {
}
