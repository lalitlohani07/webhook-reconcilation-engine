package com.example.webhookengine.ratelimit.service;

import com.example.webhookengine.exception.RateLimiterUnavailableException;
import com.example.webhookengine.ratelimit.config.RateLimitProperties;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;

@Service
public class RedisRateLimiterService implements RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiterService.class);

    private static final String RATE_LIMIT_LUA_SCRIPT = """
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local expireSeconds = tonumber(ARGV[2])

        local current = redis.call('INCR', key)
        if current == 1 then
            redis.call('EXPIRE', key, expireSeconds)
        end
        local ttl = redis.call('TTL', key)
        if ttl == -1 then
            redis.call('EXPIRE', key, expireSeconds)
        end
        return current
        """;

    private final StringRedisTemplate redisTemplate;
    private final RateLimitProperties properties;
    private final DefaultRedisScript<Long> redisScript;

    public RedisRateLimiterService(StringRedisTemplate redisTemplate, RateLimitProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.redisScript = new DefaultRedisScript<>();
        this.redisScript.setScriptText(RATE_LIMIT_LUA_SCRIPT);
        this.redisScript.setResultType(Long.class);
    }

    @Override
    public RateLimitResult tryAcquire(String rawSource) {
        if (!properties.isEnabled()) {
            return RateLimitResult.allowed(properties.getCapacity(), properties.getCapacity());
        }

        String safeSource = normalizeSource(rawSource);
        long nowSeconds = Instant.now().getEpochSecond();
        long windowSeconds = Math.max(1, properties.getWindowSeconds());
        long epochWindow = nowSeconds / windowSeconds;
        long secondsRemainingInWindow = Math.max(1, windowSeconds - (nowSeconds % windowSeconds));
        String redisKey = "app:ratelimit:src:" + safeSource + ":" + epochWindow;
        long expireSeconds = windowSeconds * 2;

        try {
            Long current = redisTemplate.execute(
                redisScript,
                Collections.singletonList(redisKey),
                String.valueOf(properties.getCapacity()),
                String.valueOf(expireSeconds)
            );

            if (current == null) {
                throw new IllegalStateException("Redis returned null result for rate limit script");
            }

            if (current <= properties.getCapacity()) {
                long remaining = properties.getCapacity() - current;
                return RateLimitResult.allowed(properties.getCapacity(), remaining);
            } else {
                return RateLimitResult.denied(properties.getCapacity(), secondsRemainingInWindow);
            }
        } catch (Exception ex) {
            if (properties.isFailClosed()) {
                log.error("Redis rate limiter unavailable while evaluating source '{}'. Failing closed per configured policy.", safeSource, ex);
                throw new RateLimiterUnavailableException("Rate limiter service unavailable", ex);
            } else {
                log.warn("Redis rate limiter unavailable while evaluating source '{}'. Failing open per configured policy.", safeSource, ex);
                return RateLimitResult.allowed(properties.getCapacity(), properties.getCapacity());
            }
        }
    }

    public String normalizeSource(String rawSource) {
        if (rawSource == null || rawSource.isBlank()) {
            return "unknown";
        }
        String trimmed = rawSource.trim().toLowerCase();
        if (trimmed.matches("^[a-z0-9_-]{1,64}$")) {
            return trimmed;
        }
        return "hash_" + sha256Hex(trimmed);
    }

    private String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(input.hashCode());
        }
    }
}
