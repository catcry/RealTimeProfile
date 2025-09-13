package com.bonyan.tps;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public final class IdExtractor {

    public static String extractIdFromMsisdn(Bucket bucket, String msisdnKey) {
        RawJsonDocument raw = bucket.get(RawJsonDocument.create(msisdnKey));
        if (raw == null) return null;

        String json = raw.content();
        if (json == null) return null;
        json = json.trim();

        // 1) Root object (supports old array form too)
        JsonObject root;
        if (json.startsWith("[")) {
            JsonArray arr = JsonArray.fromJson(json);
            if (arr == null || arr.size() == 0) return null;
            root = arr.getObject(0);
        } else {
            root = JsonObject.fromJson(json);
        }
        if (root == null) return null;

        // 2) Try the CURRENT shape first: profiles[0].id
        JsonArray profiles = root.getArray("profiles");
        if (profiles == null) {
            // 3) Fallbacks for older shapes:
            //    rtpBucket.profiles[0].id  OR  rtpBucket.rtpBucket.profiles[0].id
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
