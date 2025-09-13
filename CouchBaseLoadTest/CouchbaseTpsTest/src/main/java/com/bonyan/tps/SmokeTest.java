package com.bonyan.tps;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class SmokeTest {

    /** Run a single chained lookup: MSISDN::<msisdn> -> read id -> p::<id>. */
    public static void run(Bucket bucket, String msisdnKey) {
        long t0 = System.nanoTime();

        // 1) Extract id from MSISDN doc (supports root object, array, and legacy rtpBucket nesting)
        String id = extractIdFromMsisdn(bucket, msisdnKey);
        if (id == null || id.isEmpty()) {
            System.out.println("Could not extract 'id' from " + msisdnKey);
            return;
        }
        long t1 = System.nanoTime();

        // 2) Fetch second document p::<id>
        String pKey = "p::" + id;
        JsonDocument second = bucket.get(pKey);
        long t2 = System.nanoTime();

        if (second == null) {
            System.out.printf("SECOND NOT FOUND: msisdn=%s  id=%s  pkey=%s  t_get1=%.1fms  t_get2=%.1fms%n",
                    msisdnKey, id, pKey, (t1 - t0) / 1e6, (t2 - t1) / 1e6);
            return;
        }

        int sizeB = (second.content() == null) ? 0 : second.content().toString().length();
        System.out.printf("OK  msisdn=%s  id=%s  pkey=%s  t_get1=%.1fms  t_get2=%.1fms  sizeB=%d%n",
                msisdnKey, id, pKey, (t1 - t0) / 1e6, (t2 - t1) / 1e6, sizeB);
    }

    /**
     * Handles all observed shapes:
     * - Current:  { "profiles":[{ "id": ... }], "type":"MSISDN", "value":"..." }
     * - Legacy A: [ { ... same as object ... } ]
     * - Legacy B: { "rtpBucket": { "profiles":[{ "id": ... }] } }
     * - Legacy C: { "rtpBucket": { "rtpBucket": { "profiles":[{ "id": ... }] } } }
     */
    private static String extractIdFromMsisdn(Bucket bucket, String msisdnKey) {
        RawJsonDocument raw = bucket.get(RawJsonDocument.create(msisdnKey));
        if (raw == null) return null;

        String json = raw.content();
        if (json == null) return null;
        json = json.trim();

        // Accept top-level array or object
        JsonObject root;
        if (json.startsWith("[")) {
            JsonArray arr = JsonArray.fromJson(json);
            if (arr == null || arr.size() == 0) return null;
            root = arr.getObject(0);
        } else {
            root = JsonObject.fromJson(json);
        }
        if (root == null) return null;

        // 1) Try current shape: profiles[0].id at root
        JsonArray profiles = root.getArray("profiles");

        // 2) Fallbacks for legacy shapes: rtpBucket.profiles OR rtpBucket.rtpBucket.profiles
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
