package com.example.webhookengine.exception;

public class RateLimitExceededException extends RuntimeException {

    private final long limit;
    private final long remaining;
    private final long retryAfterSeconds;
    private final String source;

    public RateLimitExceededException(long limit, long remaining, long retryAfterSeconds, String source) {
        super("Rate limit exceeded for source '" + source + "'. Please retry after " + retryAfterSeconds + " seconds.");
        this.limit = limit;
        this.remaining = remaining;
        this.retryAfterSeconds = retryAfterSeconds;
        this.source = source;
    }

    public long getLimit() {
        return limit;
    }

    public long getRemaining() {
        return remaining;
    }

    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }

    public String getSource() {
        return source;
    }
}
