package com.example.webhookengine.reconciliation.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SimulatedSourceOfTruthClient implements SourceOfTruthClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedSourceOfTruthClient.class);

    private final Map<String, ExternalEventRecord> customRecords = new ConcurrentHashMap<>();
    private final Map<String, String> customFailures = new ConcurrentHashMap<>();

    private String buildKey(String source, String eventId) {
        return (source != null ? source.trim().toLowerCase() : "") + ":" + (eventId != null ? eventId.trim() : "");
    }

    public void registerRecord(String source, String eventId, ExternalEventRecord record) {
        customRecords.put(buildKey(source, eventId), record);
    }

    public void registerFailure(String source, String eventId, String errorMessage) {
        customFailures.put(buildKey(source, eventId), errorMessage);
    }

    public void clearOverrides() {
        customRecords.clear();
        customFailures.clear();
    }

    @Override
    public Optional<ExternalEventRecord> fetchExternalRecord(String source, String eventId) throws ExternalProviderException {
        String key = buildKey(source, eventId);

        if (customFailures.containsKey(key)) {
            String errorMsg = customFailures.get(key);
            log.warn("Simulated upstream failure triggered for {}: {}", key, errorMsg);
            throw new ExternalProviderException(errorMsg);
        }

        if (customRecords.containsKey(key)) {
            return Optional.ofNullable(customRecords.get(key));
        }

        // Deterministic patterns for development and testing
        if (eventId != null) {
            if (eventId.contains("_error")) {
                log.warn("Simulated upstream failure triggered by pattern for key: {}", key);
                throw new ExternalProviderException("Simulated upstream provider connection timeout");
            }
            if (eventId.contains("_notfound")) {
                return Optional.empty();
            }
            if (eventId.contains("_mismatch")) {
                return Optional.of(new ExternalEventRecord(
                    eventId,
                    source,
                    "FAILED",
                    "payment.failed",
                    Instant.now(),
                    Map.of("note", "Simulated status mismatch from provider")
                ));
            }
            if (eventId.contains("_corrected")) {
                return Optional.of(new ExternalEventRecord(
                    eventId,
                    source,
                    "SETTLED_WITH_ADJUSTMENT",
                    "payment.settled",
                    Instant.now(),
                    Map.of("settlementReference", "adj_" + eventId)
                ));
            }
        }

        // Default matching record for normal IDs
        return Optional.of(new ExternalEventRecord(
            eventId,
            source,
            "COMPLETED",
            "payment.succeeded",
            Instant.now(),
            Collections.emptyMap()
        ));
    }
}
