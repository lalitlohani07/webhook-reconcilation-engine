package com.example.webhookengine.reconciliation.provider;

import java.util.Optional;

public interface SourceOfTruthClient {

    Optional<ExternalEventRecord> fetchExternalRecord(String source, String eventId) throws ExternalProviderException;
}
