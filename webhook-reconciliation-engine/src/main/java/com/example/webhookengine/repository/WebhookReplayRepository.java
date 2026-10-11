package com.example.webhookengine.repository;

import com.example.webhookengine.domain.WebhookEvent;
import com.example.webhookengine.domain.WebhookReplay;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface WebhookReplayRepository extends JpaRepository<WebhookReplay, UUID> {

    List<WebhookReplay> findByWebhookEventOrderByRequestedAtDesc(WebhookEvent webhookEvent);
}
