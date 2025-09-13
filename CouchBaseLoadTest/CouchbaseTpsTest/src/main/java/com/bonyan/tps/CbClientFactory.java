package com.bonyan.tps;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.util.List;

/**
 * Creates and manages a Couchbase Java SDK 2.x client (compatible with Server 5.1).
 * - Supports optional TLS (truststore-based, server-auth only).
 * - Provides conservative timeouts suitable for older clusters.
 * - Ensures clean shutdown via a JVM shutdown hook.
 */
public class CbClientFactory {
    private final Config cfg;

    private CouchbaseEnvironment env;
    private CouchbaseCluster cluster;
    private Bucket bucket;

    public CbClientFactory(Config cfg) {
        this.cfg = cfg;
    }

    /**
     * Opens (or returns) the singleton Bucket instance.
     * Caller should not close the returned Bucket directly; call {@link #close()} instead.
     */
    public synchronized Bucket open() {
        if (bucket != null) {
            return bucket;
        }

        // Build environment with conservative timeouts for 5.x-era clusters.
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(20_000)     // bootstrap/connect timeout (ms)
                .kvTimeout(5_000)           // key-value operation timeout (ms)
                .managementTimeout(10_000)  // management REST timeout (ms)
                .queryTimeout(30_000);      // N1QL timeout (not used yet, but set)

        // Enable TLS if configured
        if (cfg.sslEnabled()) {
            builder
                    .sslEnabled(true)
                    .sslTruststoreFile(cfg.truststorePath())
                    .sslTruststorePassword(cfg.truststorePassword())
                    // Explicitly tell the client to use secure bootstrap/KV ports.
                    .bootstrapHttpEnabled(true)       // HTTPS bootstrap is used when sslEnabled=true
                    .bootstrapHttpSslPort(18091)      // mgmt HTTPS (UI/API)
                    .bootstrapCarrierSslPort(11207);  // KV over TLS
        }

        env = builder.build();

        // Seed nodes (hostnames should match cert SANs when TLS is enabled)
        List<String> seeds = cfg.hosts();

        cluster = CouchbaseCluster.create(env, seeds);
        cluster.authenticate(cfg.user(), cfg.pass());

        bucket = cluster.openBucket(cfg.bucket());

        // Ensure resources are released on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "cb-client-shutdown"));

        return bucket;
    }

    /**
     * Gracefully closes the Bucket/Cluster/Environment in the right order.
     * Safe to call multiple times.
     */
    public synchronized void close() {
        try {
            if (bucket != null) {
                bucket.close();
            }
        } catch (Exception ignored) {
        } finally {
            bucket = null;
        }

        try {
            if (cluster != null) {
                cluster.disconnect();
            }
        } catch (Exception ignored) {
        } finally {
            cluster = null;
        }

        try {
            if (env != null) {
                env.shutdown();
            }
        } catch (Exception ignored) {
        } finally {
            env = null;
        }
    }

    /** Returns the currently opened Bucket (may be null if not opened yet). */
    public synchronized Bucket currentBucket() {
        return bucket;
    }
}
