package com.bonyan.tps;

public class TokenBucket {
    private final long capacity;
    private final double refillPerNanos;
    private double tokens;
    private long lastRefill;

    public TokenBucket(long capacity, double refillPerSecond) {
        this.capacity = capacity;
        this.refillPerNanos = refillPerSecond / 1_000_000_000.0;
        this.tokens = capacity;
        this.lastRefill = System.nanoTime();
    }

    public synchronized void take() {
        for (;;) {
            refill();
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return;
            }
            try {
                // Sleep a tiny bit to avoid busy spin
                wait(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefill;
        if (elapsed <= 0) return;
        tokens = Math.min(capacity, tokens + elapsed * refillPerNanos);
        lastRefill = now;
    }
}
