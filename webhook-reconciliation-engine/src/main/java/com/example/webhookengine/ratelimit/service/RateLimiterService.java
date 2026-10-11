package com.example.webhookengine.ratelimit.service;

import com.example.webhookengine.ratelimit.dto.RateLimitResult;

public interface RateLimiterService {

    RateLimitResult tryAcquire(String source);
}
