package com.example.cbprofileutils.service;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.example.cbprofileutils.config.ProfileConfig;
import com.example.cbprofileutils.mapping.CompiledMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;

import static com.couchbase.client.java.kv.MutateInSpec.upsert;

@Service
public class ProfileDetailService {
    private static final Logger log = LoggerFactory.getLogger(ProfileDetailService.class);
    private int profileIdBatchSize;
    private int couchbaseBatchSize;
    private int threadCount;

    private final Map<String, Long> profileIdCache;
    private final Bucket mainBucket, profileIdsBucket;
    private final PsqlProfileIdsService psqlProfileIdsService;
    private final CompiledMapping mapping;
    private final JsonObject profileTemplate, msisdnTemplate;
    private final com.couchbase.client.java.Collection profileIdsCol, mainCol;

    // chunk-level executor (runs processChunk tasks)
    private final ExecutorService executor;

    // ioExecutor: for non-blocking async callbacks (kept for potential non-blocking work)
    private final ExecutorService ioExecutor;

    // blockingIoExecutor: dedicated pool for operations that may block (.join(), synchronous mutateIn, etc)
    private final ExecutorService blockingIoExecutor;

    // simple client-side limiter for outstanding KV ops
    private final Semaphore kvSemaphore;

    private Duration timeout;

    public ProfileDetailService(CouchbaseTemplate couchbaseTemplate, ProfileConfig cfg,
                                @Value("${psql-batchSize}") String profileIdBatchSize,
                                @Value("${cb-batchSize}") String couchbaseBatchSize,
                                @Value("${thread-count}") String threadCount,
                                @Value("${spring.data.couchbase.bucket-name}") String bucketName,
                                @Value("${profileIdsBucket}") String profileIdsBucketName,
                                @Value("${couchbase.operation.timeout:20}") Long timeoutStr,
                                PsqlProfileIdsService psqlProfileIdsService) {
        this.profileIdBatchSize = Integer.parseInt(profileIdBatchSize);
        this.couchbaseBatchSize = Integer.parseInt(couchbaseBatchSize);
        this.threadCount = Integer.parseInt(threadCount);
        this.profileIdCache = new ConcurrentHashMap<>(this.profileIdBatchSize * 10);

        int avail = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(Math.min(this.threadCount, Math.max(4, avail)));
        // keep an ioExecutor for light-weight tasks / callbacks
        this.ioExecutor = Executors.newFixedThreadPool(Math.max(4, avail));

        // dedicated pool for blocking IO operations; size should be tuned
        // default: threadCount * 2 or at least number of cores
        this.blockingIoExecutor = Executors.newFixedThreadPool(Math.max(this.threadCount * 2, avail));

        // limit concurrent KV ops to avoid saturating endpoints (tune this)
        this.kvSemaphore = new Semaphore(Math.max(50, this.threadCount * 10), true);

        this.psqlProfileIdsService = Objects.requireNonNull(psqlProfileIdsService);
        Cluster cluster = couchbaseTemplate.getCouchbaseClientFactory().getCluster();
        this.mainBucket = cluster.bucket(bucketName);
        this.profileIdsBucket = cluster.bucket(profileIdsBucketName);
        this.mapping = cfg.mapping();
        this.profileTemplate = cfg.profileTemplate();
        this.msisdnTemplate = cfg.msisdnTemplate();
        this.mainCol = mainBucket.defaultCollection();
        this.profileIdsCol = profileIdsBucket.defaultCollection();

        try {
            this.timeout = Duration.ofSeconds(timeoutStr);
            log.info("Using Couchbase timeout: {}", timeout);
        } catch (DateTimeParseException e) {
            log.warn("Failed to parse timeout '{}', using 5s", timeoutStr);
            this.timeout = Duration.ofSeconds(5);
        }
    }

    private static String safeGetString(JsonObject json, String key) {
        try {
            return json.getString(key);
        } catch (Exception e) {
            return null;
        }
    }

    public CompletableFuture<Void> addBIToProfileDetailsUsingBulkLoad(Map<String, JsonObject> biEntityMap) {
        Map<String, JsonObject> biEntityMapCopy = new HashMap<>(biEntityMap);
        Map<String, Long> profileIds = getProfileIdsBatch(biEntityMapCopy.keySet());
        List<Map.Entry<String, JsonObject>> entries = new ArrayList<>(biEntityMapCopy.entrySet());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < entries.size(); i += couchbaseBatchSize) {
            List<Map.Entry<String, JsonObject>> chunk = entries.subList(i, Math.min(i + couchbaseBatchSize, entries.size()));
            futures.add(CompletableFuture.runAsync(() -> processChunk(chunk, profileIds), executor));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private Map<String, Long> getProfileIdsBatch(Set<String> msisdns) {
        Map<String, Long> result = new ConcurrentHashMap<>();
        List<String> uncached = new ArrayList<>();

        for (String msisdn : msisdns) {
            Long cachedId = profileIdCache.get(msisdn);
            if (cachedId != null) result.put(msisdn, cachedId);
            else uncached.add(msisdn);
        }

        for (int i = 0; i < uncached.size(); i += profileIdBatchSize) {
            List<String> batch = uncached.subList(i, Math.min(i + profileIdBatchSize, uncached.size()));
            Map<String, Long> batchResult = psqlProfileIdsService.getOrCreateIdsBatch(batch);
            profileIdCache.putAll(batchResult);
            result.putAll(batchResult);
        }

        return result;
    }

    private void processChunk(List<Map.Entry<String, JsonObject>> chunk, Map<String, Long> profileIds) {
        List<Map.Entry<String, JsonObject>> updates = new ArrayList<>();
        List<Map.Entry<String, JsonObject>> creates = new ArrayList<>();

        // collect MSISDNs
        Set<String> batchMsisdns = new HashSet<>();
        for (Map.Entry<String, JsonObject> entry : chunk) batchMsisdns.add(entry.getKey());

        // bulk async GET (populate existingMsisdns)
        Map<String, Boolean> existingMsisdns = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> lookupFutures = new ArrayList<>();
        for (String msisdn : batchMsisdns) {
            lookupFutures.add(
                    profileIdsCol.async().get(msisdn)
                            .thenAccept(doc -> existingMsisdns.put(msisdn, true))
                            .exceptionally(ex -> { existingMsisdns.put(msisdn, false); return null; })
            );
        }
        CompletableFuture.allOf(lookupFutures.toArray(new CompletableFuture[0])).join();

        for (Map.Entry<String, JsonObject> entry : chunk) {
            String msisdn = entry.getKey();
            boolean exists = existingMsisdns.getOrDefault(msisdn, false);
            if (exists) updates.add(entry); else creates.add(entry);
        }

        processUpdates(updates, profileIds);
        processCreates(creates, profileIds);
    }

    private void processUpdates(List<Map.Entry<String, JsonObject>> updates, Map<String, Long> profileIds) {
        if (updates.isEmpty()) return;
        List<CompletableFuture<Void>> futures = new ArrayList<>(updates.size());
        for (Map.Entry<String, JsonObject> entry : updates) {
            // Use blockingIoExecutor because mutateIn(...) is executed synchronously inside retry
            futures.add(CompletableFuture.runAsync(() -> {
                String msisdn = entry.getKey();
                JsonObject row = entry.getValue();
                Long profileId = profileIds.get("FAA_" + msisdn);
                String profileKey = mapping.profileKeyPrefix + profileId;
                try {
                    // acquire permit to limit outstanding KV ops
                    kvSemaphore.acquire();
                    try {
                        List<MutateInSpec> specs = buildSpecsFromMapping(row);
                        if (!specs.isEmpty()) {
                            retryTimeoutBackoff(3, () ->
                                    mainCol.mutateIn(profileKey, specs, MutateInOptions.mutateInOptions().timeout(timeout))
                            );
                        }
                    } finally {
                        kvSemaphore.release();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("Error updating MSISDN {}", msisdn, e);
                }
            }, blockingIoExecutor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void processCreates(List<Map.Entry<String, JsonObject>> creates, Map<String, Long> profileIds) {
        if (creates.isEmpty()) return;
        int createBatchSize = Math.max(1, couchbaseBatchSize / 2);
        for (int i = 0; i < creates.size(); i += createBatchSize) {
            List<Map.Entry<String, JsonObject>> batch = creates.subList(i, Math.min(i + createBatchSize, creates.size()));
            List<CompletableFuture<Void>> batchFutures = new ArrayList<>(batch.size());
            for (Map.Entry<String, JsonObject> entry : batch) {
                // these tasks may block due to .join() in the async chain and the retry wrapper
                batchFutures.add(CompletableFuture.runAsync(() -> {
                    String msisdn = entry.getKey();
                    JsonObject row = entry.getValue();
                    Long profileId = profileIds.get("FAA_" + msisdn);
                    String profileKey = mapping.profileKeyPrefix + profileId;
                    try {
                        JsonObject profileDoc = minimalProfileDoc(profileTemplate, row, profileKey, msisdn);
                        JsonObject msisdnDoc = minimalMsisdnDoc(msisdnTemplate, msisdn, profileKey);
                        JsonObject ref = JsonObject.create().put("id", profileKey);

                        // limit outstanding KV operations
                        kvSemaphore.acquire();
                        try {
                            retryTimeoutBackoff(3, () ->
                                            mainCol.async().insert(profileKey, profileDoc)
                                                    .thenCombine(mainCol.async().insert(mapping.msisdnKeyPrefix + msisdn, msisdnDoc), (a,b)->null)
//                                            .thenCombine(tempCol.async().insert(msisdn, ref), (a,b)->null)
                                                    .thenCombine(profileIdsCol.async().insert(msisdn, ref), (a,b)->null)
                                                    .exceptionally(ex -> { if (!(ex.getCause() instanceof DocumentExistsException)) log.error("Error creating docs for {}", msisdn, ex); return null; })
                                                    .join()
                            );
                        } finally {
                            kvSemaphore.release();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error("Error creating MSISDN {}", msisdn, e);
                    }
                }, blockingIoExecutor));
            }
            CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).join();
        }
    }

    private List<MutateInSpec> buildSpecsFromMapping(JsonObject row) {
        List<MutateInSpec> specs = new ArrayList<>(mapping.fields.size() + 1);
        for (CompiledMapping.FieldSpec f : mapping.fields) {
            String raw = safeGetString(row, f.biHeader);
            if (raw == null || raw.isEmpty()) continue;
            Object val = CompiledMapping.convert(f, raw);
            if (val != null) specs.add(upsert(f.path, val).createPath());
        }
        if (mapping.rawSavePath != null) specs.add(upsert(mapping.rawSavePath, row).createPath());
        return specs;
    }

    private JsonObject minimalProfileDoc(JsonObject baseTemplate, JsonObject row, String profileKey, String msisdn) {
        JsonObject doc = JsonObject.create()
                .put("id", profileKey)
                .put("name", "FAA_" + msisdn)
                .put("type", "FAA")
                .put("attrGrps", JsonObject.create());
        for (CompiledMapping.FieldSpec f : mapping.fields) {
            String raw = safeGetString(row, f.biHeader);
            if (raw == null || raw.isEmpty()) continue;
            Object val = CompiledMapping.convert(f, raw);
            if (val != null) CompiledMapping.put(doc, f.pathTokens, val);
        }
        if (mapping.rawSavePath != null) CompiledMapping.put(doc, mapping.rawSavePath.split("\\."), row);
        return doc;
    }

    private JsonObject minimalMsisdnDoc(JsonObject baseTemplate, String msisdn, String profileKey) {
        JsonObject doc = JsonObject.create().put("type", "MSISDN").put("value", msisdn);
        JsonArray profiles = JsonArray.create().add(JsonObject.create().put("attr", JsonArray.create())
                .put("id", profileKey.substring(3))
                .put("priority", 1)
                .put("roles", JsonArray.create()));
        doc.put("profiles", profiles);
        return doc;
    }

    private void retryTimeoutBackoff(int maxAttempts, Runnable op) {
        int n = 0;
        long delayMs = 100;
        while (true) {
            try {
                op.run();
                return;
            } catch (TimeoutException te) {
                if (++n >= maxAttempts) throw te;
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                delayMs = Math.min(delayMs * 2, 1000);
            }
        }
    }

    @PreDestroy
    public void shutdownExecutors() {
        try {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            ioExecutor.shutdown();
            ioExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            blockingIoExecutor.shutdown();
            blockingIoExecutor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
