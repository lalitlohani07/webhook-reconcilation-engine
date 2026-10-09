package com.example.webhookengine.repository;

import com.example.webhookengine.domain.WebhookAttempt;
import com.example.webhookengine.domain.WebhookEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WebhookAttemptRepository extends JpaRepository<WebhookAttempt, UUID> {

    List<WebhookAttempt> findByWebhookEventOrderByAttemptNumberAsc(WebhookEvent webhookEvent);

    Optional<WebhookAttempt> findByWebhookEventAndAttemptNumber(WebhookEvent webhookEvent, int attemptNumber);
}
