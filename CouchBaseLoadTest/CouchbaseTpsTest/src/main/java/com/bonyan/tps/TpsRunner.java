package com.bonyan.tps;

import com.couchbase.client.java.Bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TpsRunner {
    public static class Options {
        public final int threads;
        public final int durationSeconds;
        public final long queueCapacity;
        public final Double maxRatePerSec; // null = unlimited
        public Options(int threads, int durationSeconds, long queueCapacity, Double maxRatePerSec) {
            this.threads = threads;
            this.durationSeconds = durationSeconds;
            this.queueCapacity = queueCapacity;
            this.maxRatePerSec = maxRatePerSec;
        }
    }

    public static void run(Bucket bucket, String filePath, Options opts) throws Exception {
        BlockingQueue<String> queue = new ArrayBlockingQueue<>((int) opts.queueCapacity);
        Metrics metrics = new Metrics();

        MsisdnProducer producer = new MsisdnProducer(filePath, queue);
        Thread producerThread = new Thread(producer, "msisdn-producer");

        TokenBucket limiter = null;
        if (opts.maxRatePerSec != null && opts.maxRatePerSec > 0) {
            limiter = new TokenBucket(Math.max(1, opts.maxRatePerSec.intValue()), opts.maxRatePerSec);
        }

        ExecutorService pool = Executors.newFixedThreadPool(opts.threads);
        List<Future<?>> workers = new ArrayList<>();
        for (int i = 0; i < opts.threads; i++) {
            workers.add(pool.submit(new Worker(bucket, queue, metrics, limiter)));
        }

        ScheduledExecutorService stats = Executors.newSingleThreadScheduledExecutor();
        final long[] lastOk  = {0};
        final long[] lastNf1 = {0};
        final long[] lastNf2 = {0};
        final long[] lastErr = {0};
        stats.scheduleAtFixedRate(() -> {
            Metrics.Snapshot s = metrics.snapshotAndReset(
                    lastOk[0], lastNf1[0], lastNf2[0], lastErr[0]);

            lastOk[0]  = s.okTotal;
            lastNf1[0] = s.nf1Total;
            lastNf2[0] = s.nf2Total;
            lastErr[0] = s.errTotal;

            long tps = s.okDelta + s.nf1Delta + s.nf2Delta + s.errDelta;

            System.out.printf(
                    "tps=%d ok=%d nf1=%d nf2=%d err=%d inflight=%d avgLatencyMs=%.2f total[ok=%d nf1=%d nf2=%d err=%d]%n",
                    tps, s.okDelta, s.nf1Delta, s.nf2Delta, s.errDelta,
                    s.inFlight, s.avgLatencyMs,
                    s.okTotal, s.nf1Total, s.nf2Total, s.errTotal
            );
        }, 1, 1, TimeUnit.SECONDS);

        System.out.printf("Starting: threads=%d, duration=%ds, rate=%s, queue=%d%n",
                opts.threads, opts.durationSeconds,
                (opts.maxRatePerSec == null ? "unlimited" : opts.maxRatePerSec.toString()),
                opts.queueCapacity);

        // ---- start the run ----
        producerThread.start();

        // Run for duration, then stop
        TimeUnit.SECONDS.sleep(opts.durationSeconds);

        // ---- teardown ----
        producer.stop();
        producerThread.interrupt();
        stats.shutdownNow();
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Final totals after stopping everything
        Metrics.Snapshot fin = metrics.snapshotAndReset(
                lastOk[0], lastNf1[0], lastNf2[0], lastErr[0]);

        System.out.printf(
                "SUMMARY  total_ok=%d  total_nf1(first)=%d  total_nf2(second)=%d  total_err=%d%n",
                fin.okTotal, fin.nf1Total, fin.nf2Total, fin.errTotal
        );

        System.out.println("Finished load run.");
    }
}
