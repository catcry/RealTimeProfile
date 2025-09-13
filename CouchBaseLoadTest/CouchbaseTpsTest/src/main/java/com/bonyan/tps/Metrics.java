package com.bonyan.tps;

import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong nfFirst = new AtomicLong();   // first hop (MSISDN) not found
    private final AtomicLong nfSecond = new AtomicLong();  // second hop (p::<id>) not found
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong inFlight = new AtomicLong();

    // latency (ns)
    private final AtomicLong latSumNs = new AtomicLong();
    private final AtomicLong latCount = new AtomicLong();

    public void incInFlight() { inFlight.incrementAndGet(); }
    public void decInFlight() { inFlight.decrementAndGet(); }

    public void recordOk(long latencyNs) {
        ok.incrementAndGet();
        latSumNs.addAndGet(latencyNs);
        latCount.incrementAndGet();
    }

    public void recordNotFoundFirst(long latencyNs) {
        nfFirst.incrementAndGet();
        latSumNs.addAndGet(latencyNs);
        latCount.incrementAndGet();
    }

    public void recordNotFoundSecond(long latencyNs) {
        nfSecond.incrementAndGet();
        latSumNs.addAndGet(latencyNs);
        latCount.incrementAndGet();
    }

    public void recordError() { errors.incrementAndGet(); }

    public Snapshot snapshotAndReset(long lastOk, long lastNf1, long lastNf2, long lastErrors) {
        long okNow = ok.get();
        long nf1Now = nfFirst.get();
        long nf2Now = nfSecond.get();
        long errNow = errors.get();

        long okDelta = okNow - lastOk;
        long nf1Delta = nf1Now - lastNf1;
        long nf2Delta = nf2Now - lastNf2;
        long errDelta = errNow - lastErrors;

        long c = latCount.getAndSet(0);
        long s = latSumNs.getAndSet(0);
        double avgMs = c == 0 ? 0.0 : (s / 1e6) / c;

        return new Snapshot(okNow, nf1Now, nf2Now, errNow, okDelta, nf1Delta, nf2Delta, errDelta, inFlight.get(), avgMs);
    }

    public static class Snapshot {
        public final long okTotal, nf1Total, nf2Total, errTotal;
        public final long okDelta, nf1Delta, nf2Delta, errDelta;
        public final long inFlight;
        public final double avgLatencyMs;

        public Snapshot(long okTotal, long nf1Total, long nf2Total, long errTotal,
                        long okDelta, long nf1Delta, long nf2Delta, long errDelta,
                        long inFlight, double avgLatencyMs) {
            this.okTotal = okTotal; this.nf1Total = nf1Total; this.nf2Total = nf2Total; this.errTotal = errTotal;
            this.okDelta = okDelta; this.nf1Delta = nf1Delta; this.nf2Delta = nf2Delta; this.errDelta = errDelta;
            this.inFlight = inFlight; this.avgLatencyMs = avgLatencyMs;
        }
    }
}
