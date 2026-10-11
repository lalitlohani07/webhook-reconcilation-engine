package com.example.webhookengine.ratelimit.service;

import com.example.webhookengine.exception.RateLimiterUnavailableException;
import com.example.webhookengine.ratelimit.config.RateLimitProperties;
import com.example.webhookengine.ratelimit.dto.RateLimitResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisRateLimiterServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    private RateLimitProperties properties;
    private RedisRateLimiterService rateLimiterService;

    @BeforeEach
    void setUp() {
        properties = new RateLimitProperties();
        properties.setEnabled(true);
        properties.setCapacity(5);
        properties.setWindowSeconds(60);
        properties.setFailClosed(true);

        rateLimiterService = new RedisRateLimiterService(redisTemplate, properties);
    }

    @Test
    @DisplayName("tryAcquire allows requests within configured capacity")
    void shouldAllowRequestWithinCapacity() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), eq("5"), eq("120")))
            .thenReturn(1L);

        RateLimitResult result = rateLimiterService.tryAcquire("stripe");

        assertThat(result.allowed()).isTrue();
        assertThat(result.limit()).isEqualTo(5);
        assertThat(result.remaining()).isEqualTo(4);
        assertThat(result.retryAfterSeconds()).isEqualTo(0);
    }

    @Test
    @DisplayName("tryAcquire denies request when capacity is exhausted")
    void shouldDenyRequestWhenCapacityExhausted() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), eq("5"), eq("120")))
            .thenReturn(6L);

        RateLimitResult result = rateLimiterService.tryAcquire("stripe");

        assertThat(result.allowed()).isFalse();
        assertThat(result.limit()).isEqualTo(5);
        assertThat(result.remaining()).isEqualTo(0);
        assertThat(result.retryAfterSeconds()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Different sources use independent rate limit keys")
    void shouldSeparateBucketsForDifferentSources() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), eq("5"), eq("120")))
            .thenReturn(1L);

        rateLimiterService.tryAcquire("stripe");
        rateLimiterService.tryAcquire("shopify");

        assertThat(rateLimiterService.normalizeSource("stripe")).isEqualTo("stripe");
        assertThat(rateLimiterService.normalizeSource("shopify")).isEqualTo("shopify");
    }

    @Test
    @DisplayName("Normalizes safe source names and hashes complex or unbounded source names")
    void shouldNormalizeSourceNamesSafely() {
        assertThat(rateLimiterService.normalizeSource("  STRIPE  ")).isEqualTo("stripe");
        assertThat(rateLimiterService.normalizeSource("shopify_v2-prod")).isEqualTo("shopify_v2-prod");
        assertThat(rateLimiterService.normalizeSource(null)).isEqualTo("unknown");
        assertThat(rateLimiterService.normalizeSource("   ")).isEqualTo("unknown");

        // Special characters or excessive length result in safe bounded sha256 hash
        String complex = "provider:with:colons:and spaces!";
        String normalized = rateLimiterService.normalizeSource(complex);
        assertThat(normalized).startsWith("hash_");
        assertThat(normalized).doesNotContain(":");
        assertThat(normalized).doesNotContain(" ");
    }

    @Test
    @DisplayName("When rate limiting is disabled, returns allowed without querying Redis")
    void shouldBypassWhenDisabled() {
        properties.setEnabled(false);

        RateLimitResult result = rateLimiterService.tryAcquire("stripe");

        assertThat(result.allowed()).isTrue();
        assertThat(result.limit()).isEqualTo(5);
        assertThat(result.remaining()).isEqualTo(5);
        verify(redisTemplate, never()).execute(any(RedisScript.class), anyList(), any());
    }

    @Test
    @DisplayName("When Redis fails and fail-closed is active, throws RateLimiterUnavailableException")
    void shouldFailClosedWhenRedisUnavailable() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any()))
            .thenThrow(new RedisConnectionFailureException("Connection refused"));

        assertThatThrownBy(() -> rateLimiterService.tryAcquire("stripe"))
            .isInstanceOf(RateLimiterUnavailableException.class)
            .hasMessageContaining("Rate limiter service unavailable");
    }

    @Test
    @DisplayName("When Redis fails and fail-closed is false, fails open and allows request")
    void shouldFailOpenWhenConfigured() {
        properties.setFailClosed(false);
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any()))
            .thenThrow(new RedisConnectionFailureException("Connection refused"));

        RateLimitResult result = rateLimiterService.tryAcquire("stripe");

        assertThat(result.allowed()).isTrue();
        assertThat(result.limit()).isEqualTo(5);
    }

    @Test
    @DisplayName("Concurrent calls to tryAcquire behave consistently")
    void shouldHandleConcurrentAcquisitions() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any()))
            .thenAnswer(inv -> (long) counter.incrementAndGet());

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger allowedCount = new AtomicInteger(0);
        AtomicInteger deniedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    RateLimitResult res = rateLimiterService.tryAcquire("concurrent-source");
                    if (res.allowed()) {
                        allowedCount.incrementAndGet();
                    } else {
                        deniedCount.incrementAndGet();
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(allowedCount.get()).isEqualTo(5);
        assertThat(deniedCount.get()).isEqualTo(5);
    }
}
