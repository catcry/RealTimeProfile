package com.bonyan.tps;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.concurrent.BlockingQueue;

public class Worker implements Runnable {
    private final Bucket bucket;
    private final BlockingQueue<String> queue;
    private final Metrics metrics;
    private final TokenBucket rateLimiter; // nullable

    public Worker(Bucket bucket,
                  BlockingQueue<String> queue,
                  Metrics metrics,
                  TokenBucket rateLimiter) {
        this.bucket = bucket;
        this.queue = queue;
        this.metrics = metrics;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                String msisdnKey = queue.take(); // blocks
                if (rateLimiter != null) rateLimiter.take();

                metrics.incInFlight();
                long t0 = System.nanoTime();
                try {
                    // --- First hop: MSISDN::<number> (top-level ARRAY or OBJECT) ---
                    String id = extractIdFromMsisdn(bucket, msisdnKey);
                    if (id == null || id.isEmpty()) {
                        // If document truly missing or id not found → count as nf1
                        metrics.recordNotFoundFirst(System.nanoTime() - t0);
                        continue;
                    }

                    // --- Second hop: p::<id> (JSON object) ---
                    String pKey = "p::" + id;
                    JsonDocument b = bucket.get(pKey);
                    if (b == null) {
                        metrics.recordNotFoundSecond(System.nanoTime() - t0);
                    } else {
                        metrics.recordOk(System.nanoTime() - t0);
                    }

                } catch (Exception e) {
                    metrics.recordError();
                } finally {
                    metrics.decInFlight();
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------- helpers ----------

    private static String extractIdFromMsisdn(Bucket bucket, String msisdnKey) {
        // Fetch raw JSON so we can handle top-level ARRAY or OBJECT
        RawJsonDocument raw = bucket.get(RawJsonDocument.create(msisdnKey));
        if (raw == null) return null;

        String json = raw.content();
        if (json == null) return null;

        json = json.trim();
        JsonObject root;
        if (json.startsWith("[")) {
            JsonArray arr = JsonArray.fromJson(json);
            if (arr == null || arr.size() == 0) return null;
            root = arr.getObject(0);
        } else {
            root = JsonObject.fromJson(json);
        }
        if (root == null) return null;

        // Current shape: { "profiles": [ { "id": ... } ], ... }
        JsonArray profiles = root.getArray("profiles");

        // Legacy fallbacks:
        //  - { "rtpBucket": { "profiles": [ ... ] } }
        //  - { "rtpBucket": { "rtpBucket": { "profiles": [ ... ] } } }
        if (profiles == null) {
            JsonObject rtp1 = root.getObject("rtpBucket");
            if (rtp1 != null) {
                profiles = rtp1.getArray("profiles");
                if (profiles == null) {
                    JsonObject rtp2 = rtp1.getObject("rtpBucket");
                    if (rtp2 != null) {
                        profiles = rtp2.getArray("profiles");
                    }
                }
            }
        }
        if (profiles == null || profiles.size() == 0) return null;

        JsonObject p0 = profiles.getObject(0);
        if (p0 == null) return null;

        Object idVal = p0.get("id");
        if (idVal == null) return null;

        if (idVal instanceof String) return (String) idVal;
        if (idVal instanceof Number)  return numberToString((Number) idVal);
        return String.valueOf(idVal);
    }

    private static String numberToString(Number n) {
        long lv = n.longValue();
        return (Double.compare(n.doubleValue(), (double) lv) == 0)
                ? Long.toString(lv)
                : n.toString();
    }
}
