package com.example.webhookengine.service;

import com.example.webhookengine.config.RetryProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Exponential backoff policy calculator for webhook event retries.
 * <p>
 * Formula:
 * d_n = min(d_0 * m^(n - 1), d_max)
 * <p>
 * Where:
 * - d_n: delay before the next execution
 * - d_0: initial delay (e.g. 1000ms)
 * - m: multiplier (e.g. 2.0)
 * - n: failed execution attempt number (1-based: 1, 2, 3...)
 * - d_max: maximum delay cap (e.g. 60000ms)
 */
@Component
public class RetryPolicy {

    private final RetryProperties properties;

    public RetryPolicy(RetryProperties properties) {
        this.properties = properties;
    }

    /**
     * Determines whether another processing attempt is allowed given the failed execution number.
     * With maxAttempts = 5:
     * failedExecution 1..4 -> canRetry = true
     * failedExecution 5 -> canRetry = false (exhausted)
     *
     * @param failedExecutionNumber the 1-based attempt number that just failed
     * @return true if another attempt can be scheduled, false if max attempts reached or retry disabled
     */
    public boolean canRetry(int failedExecutionNumber) {
        if (!properties.isEnabled()) {
            return false;
        }
        return failedExecutionNumber < properties.getMaxAttempts();
    }

    /**
     * Calculates the backoff delay duration for the next retry following failedExecutionNumber.
     *
     * @param failedExecutionNumber 1-based failed execution number (n >= 1)
     * @return backoff Duration (guaranteed >= 0 and <= maxDelayMs)
     */
    public Duration calculateBackoffDelay(int failedExecutionNumber) {
        long initialDelay = Math.max(0L, properties.getInitialDelayMs());
        long maxDelay = Math.max(initialDelay, properties.getMaxDelayMs());
        double multiplier = properties.getMultiplier() > 0 ? properties.getMultiplier() : 1.0;

        if (failedExecutionNumber <= 1) {
            return Duration.ofMillis(Math.min(initialDelay, maxDelay));
        }

        // d_n = min(d_0 * m^(n - 1), d_max)
        double exponent = failedExecutionNumber - 1;
        double factor = Math.pow(multiplier, exponent);

        // Guard against overflow
        double calculatedDelay = (double) initialDelay * factor;
        if (Double.isInfinite(calculatedDelay) || Double.isNaN(calculatedDelay) || calculatedDelay > (double) maxDelay) {
            return Duration.ofMillis(maxDelay);
        }

        long delayMs = Math.min((long) calculatedDelay, maxDelay);
        return Duration.ofMillis(Math.max(0L, delayMs));
    }

    /**
     * Calculates the Instant in UTC at which the next retry execution should occur.
     *
     * @param failedExecutionNumber 1-based failed execution number
     * @param fromTime reference time (e.g. Instant.now())
     * @return scheduled Instant for the next retry
     */
    public Instant calculateNextRetryAt(int failedExecutionNumber, Instant fromTime) {
        Instant base = fromTime != null ? fromTime : Instant.now();
        return base.plus(calculateBackoffDelay(failedExecutionNumber));
    }

    public RetryProperties getProperties() {
        return properties;
    }
}
