package com.example.webhookengine.service;

import com.example.webhookengine.config.RetryProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    private RetryProperties properties;
    private RetryPolicy retryPolicy;

    @BeforeEach
    void setUp() {
        properties = new RetryProperties();
        properties.setEnabled(true);
        properties.setMaxAttempts(5);
        properties.setInitialDelayMs(1000L);
        properties.setMultiplier(2.0);
        properties.setMaxDelayMs(60000L);
        retryPolicy = new RetryPolicy(properties);
    }

    @ParameterizedTest(name = "Failed attempt {0} -> delay {1} ms")
    @CsvSource({
        "1, 1000",
        "2, 2000",
        "3, 4000",
        "4, 8000",
        "5, 16000",
        "6, 32000",
        "7, 60000" // Capped at maxDelayMs
    })
    @DisplayName("Calculates exponential backoff delay correctly according to formula: d_n = min(d_0 * m^(n-1), d_max)")
    void shouldCalculateExpectedBackoffDelay(int failedAttempt, long expectedDelayMs) {
        Duration delay = retryPolicy.calculateBackoffDelay(failedAttempt);
        assertThat(delay.toMillis()).isEqualTo(expectedDelayMs);
    }

    @Test
    @DisplayName("Enforces maximum delay cap")
    void shouldEnforceMaxDelayCap() {
        properties.setMaxDelayMs(10000L); // 10s cap
        Duration delay = retryPolicy.calculateBackoffDelay(10);
        assertThat(delay.toMillis()).isEqualTo(10000L);
    }

    @Test
    @DisplayName("Safely handles zero or negative attempt number without error")
    void shouldHandleNonPositiveAttemptNumber() {
        Duration delay0 = retryPolicy.calculateBackoffDelay(0);
        Duration delayNegative = retryPolicy.calculateBackoffDelay(-5);

        assertThat(delay0.toMillis()).isEqualTo(1000L);
        assertThat(delayNegative.toMillis()).isEqualTo(1000L);
    }

    @Test
    @DisplayName("Safely handles large attempt number avoiding integer/double overflow")
    void shouldPreventOverflowOnLargeAttempt() {
        Duration delay = retryPolicy.calculateBackoffDelay(100);
        assertThat(delay.toMillis()).isEqualTo(60000L);
    }

    @Test
    @DisplayName("Safely normalizes invalid configuration values (negative delay, non-positive multiplier)")
    void shouldNormalizeInvalidConfiguration() {
        properties.setInitialDelayMs(-500L);
        properties.setMultiplier(-1.0);
        properties.setMaxDelayMs(-1000L);

        Duration delay = retryPolicy.calculateBackoffDelay(2);
        assertThat(delay.toMillis()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("canRetry returns true up to maxAttempts - 1 and false at maxAttempts (no off-by-one)")
    void shouldEnforceMaxAttemptsCountingConvention() {
        // maxAttempts = 5: executions 1, 2, 3, 4 can retry; execution 5 cannot retry (exhausted)
        assertThat(retryPolicy.canRetry(1)).isTrue();
        assertThat(retryPolicy.canRetry(2)).isTrue();
        assertThat(retryPolicy.canRetry(3)).isTrue();
        assertThat(retryPolicy.canRetry(4)).isTrue();
        assertThat(retryPolicy.canRetry(5)).isFalse();
        assertThat(retryPolicy.canRetry(6)).isFalse();
    }

    @Test
    @DisplayName("canRetry returns false when retry is disabled in configuration")
    void shouldNotRetryWhenDisabled() {
        properties.setEnabled(false);
        assertThat(retryPolicy.canRetry(1)).isFalse();
    }

    @Test
    @DisplayName("Calculates nextRetryAt as UTC Instant correctly")
    void shouldCalculateNextRetryAt() {
        Instant now = Instant.parse("2026-10-09T10:00:00Z");
        Instant nextRetry = retryPolicy.calculateNextRetryAt(1, now);
        assertThat(nextRetry).isEqualTo(Instant.parse("2026-10-09T10:00:01Z"));

        Instant nextRetry2 = retryPolicy.calculateNextRetryAt(2, now);
        assertThat(nextRetry2).isEqualTo(Instant.parse("2026-10-09T10:00:02Z"));
    }
}
