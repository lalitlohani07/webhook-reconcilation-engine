package com.example.webhookengine.ratelimit.dto;

public record RateLimitResult(
    boolean allowed,
    long limit,
    long remaining,
    long retryAfterSeconds
) {
    public static RateLimitResult allowed(long limit, long remaining) {
        return new RateLimitResult(true, limit, Math.max(0, remaining), 0);
    }

    public static RateLimitResult denied(long limit, long retryAfterSeconds) {
        return new RateLimitResult(false, limit, 0, Math.max(1, retryAfterSeconds));
    }
}
