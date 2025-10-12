package com.example.cbprofileutils.service;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.example.cbprofileutils.config.ProfileConfig;
import com.example.cbprofileutils.mapping.CompiledMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.couchbase.client.java.kv.LookupInSpec.get;
import static com.couchbase.client.java.kv.MutateInSpec.upsert;

@Service
public class ProfileDetailService {

    private static final Logger log = LoggerFactory.getLogger(ProfileDetailService.class);

    private final Bucket mainBucket;
    private final Bucket profileIdsBucket;
    private final Bucket tempBucket;

    private final CompiledMapping mapping;
    private final JsonObject profileTemplate;
    private final JsonObject msisdnTemplate;

    public ProfileDetailService(
            CouchbaseTemplate couchbaseTemplate,
            ProfileConfig cfg,
            @Value("${spring.data.couchbase.bucket-name}") String bucketName,
            @Value("${profileIdsBucket}") String profileIdsBucketName,
            @Value("${profileIdsTemporaryBucket}") String profileIdsTemporaryBucketName
    ) {
        Cluster cluster = couchbaseTemplate.getCouchbaseClientFactory().getCluster();
        this.mainBucket = cluster.bucket(bucketName);
        this.profileIdsBucket = cluster.bucket(profileIdsBucketName);
        this.tempBucket = cluster.bucket(profileIdsTemporaryBucketName);

        this.mapping = cfg.mapping();
        this.profileTemplate = cfg.profileTemplate();
        this.msisdnTemplate = cfg.msisdnTemplate();
    }

    public void addBIToProfileDetailsUsingBulkLoad(Map<String, JsonObject> biEntityMap) {
        Collection profileIds = profileIdsBucket.defaultCollection();
        Collection temp = tempBucket.defaultCollection();
        Collection main = mainBucket.defaultCollection();

        // Process each MSISDN independently (callers already batch/parallelize)
        for (Map.Entry<String, JsonObject> e : biEntityMap.entrySet()) {
            JsonObject biJson = e.getValue();

            // get MSISDN from mapping.idField (fallback to "MSISDN")
            String msisdn = safeGetString(biJson, mapping.idField);
            if (msisdn == null || msisdn.length() < 3) continue;

            String profileKey = mapping.profileKeyPrefix + msisdn; // deterministic

            try {
                // check if profile id mapping exists (cheap)
                boolean exists = profileIds.exists(msisdn).exists();

                if (exists) {
                    // fetch mapped profile id (could be same deterministic key)
                    var lambdaContext = new Object() {
                        String existingKey;
                    };
                    try {
                        LookupInResult look = profileIds.lookupIn(msisdn, Collections.singletonList(get("id")));
                        lambdaContext.existingKey = look.contentAs(0, String.class);
                    } catch (DocumentNotFoundException dnfe) {
                        // mapping missing but exists() returned true (rare race) → fall back
                        lambdaContext.existingKey = profileKey;
                    }

                    // build mutateIn specs for all mapped fields in one go
                    List<MutateInSpec> specs = buildSpecsFromMapping(biJson);
                    if (specs.isEmpty()) continue;

                    retryWithBackoff(3, () -> main.mutateIn(lambdaContext.existingKey, specs));

                } else {
                    // create new docs using templates + mapping
                    Map<String, String> row = jsonToStringMap(biJson);

                    JsonObject profileDoc = mapping.apply(row, profileTemplate, mapping.rawSavePath != null);
                    profileDoc.put("id", profileKey);              // full key as id if you want
                    profileDoc.put("name", "FAA_" + msisdn);

                    JsonObject msisdnDoc = CompiledMapping.deepCopy(msisdnTemplate);
                    msisdnDoc.put("value", msisdn);
                    JsonArray profiles = msisdnDoc.getArray("profiles");
                    if (profiles != null && !profiles.isEmpty()) {
                        profiles.getObject(0).put("id", profileKey.substring(3)); // id without "p::"
                    }

                    JsonObject ref = JsonObject.create().put("id", profileKey);

                    // insert with small saga; last write is mapping to avoid dangling reference
                    retryWithBackoff(3, () -> {
                        main.insert(profileKey, profileDoc);
                        main.insert(mapping.msisdnKeyPrefix + msisdn, msisdnDoc);
                        temp.insert(msisdn, ref);
                        profileIds.insert(msisdn, ref); // last
                    });
                }
            } catch (DocumentExistsException dex) {
                // benign in concurrent runs; move on
            } catch (Exception ex) {
                log.error("Failed processing MSISDN {}: {}", msisdn, ex.toString(), ex);
            }
        }
    }

    private List<MutateInSpec> buildSpecsFromMapping(JsonObject biJson) {
        Map<String,String> row = jsonToStringMap(biJson);
        List<MutateInSpec> specs = new ArrayList<>(mapping.fields.size() + 1);

        for (CompiledMapping.FieldSpec f : mapping.fields) {
            String raw = row.get(f.biHeader);
            if (raw == null || raw.isEmpty()) continue;
            Object val = CompiledMapping.convert(f, raw);
            if (val != null) {
                String path = String.join(".", f.pathTokens);
                specs.add(upsert(path, val).createPath());
            }
        }
        if (mapping.rawSavePath != null) {
            specs.add(upsert(mapping.rawSavePath, biJson).createPath());
        }
        return specs;
    }

    private static Map<String,String> jsonToStringMap(JsonObject json) {
        return json.getNames().stream()
                .collect(Collectors.toMap(k -> k, json::getString, (a,b)->a, LinkedHashMap::new));
    }

    private static String safeGetString(JsonObject json, String key) {
        try { return json.getString(key); } catch (Exception e) { return null; }
    }

    private void retryWithBackoff(int maxAttempts, Runnable task) {
        int attempts = 0;
        long delay = 200L;
        while (true) {
            try {
                task.run();
                return;
            } catch (TimeoutException e) {
                if (++attempts >= maxAttempts) throw e;
                try { Thread.sleep(delay); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                delay = Math.min(delay * 2, 2000);
            }
        }
    }
}
