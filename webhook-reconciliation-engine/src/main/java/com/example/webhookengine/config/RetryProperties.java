package com.example.webhookengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.retry")
public class RetryProperties {

    /**
     * Whether retry handling and the retry scheduler are enabled.
     */
    private boolean enabled = true;

    /**
     * Maximum number of processing executions, including the initial execution.
     * Default: 5.
     */
    private int maxAttempts = 5;

    /**
     * Initial backoff delay in milliseconds before the first retry (failed execution 1).
     * Default: 1000ms.
     */
    private long initialDelayMs = 1000L;

    /**
     * Exponential multiplier.
     * Default: 2.0.
     */
    private double multiplier = 2.0;

    /**
     * Maximum backoff delay cap in milliseconds.
     * Default: 60000ms (60s).
     */
    private long maxDelayMs = 60000L;

    /**
     * Polling interval in milliseconds for the retry scheduler.
     * Default: 1000ms.
     */
    private long schedulerIntervalMs = 1000L;

    /**
     * Number of eligible events to claim and schedule per batch.
     * Default: 20.
     */
    private int batchSize = 20;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public void setInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public void setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
    }

    public long getSchedulerIntervalMs() {
        return schedulerIntervalMs;
    }

    public void setSchedulerIntervalMs(long schedulerIntervalMs) {
        this.schedulerIntervalMs = schedulerIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
