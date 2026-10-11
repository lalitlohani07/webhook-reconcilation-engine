package com.example.webhookengine.reconciliation.provider;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public record ExternalEventRecord(
    String eventId,
    String source,
    String externalStatus,
    String eventType,
    Instant occurredAt,
    Map<String, Object> details
) {
    public ExternalEventRecord(String eventId, String source, String externalStatus) {
        this(eventId, source, externalStatus, null, Instant.now(), Collections.emptyMap());
    }
}
