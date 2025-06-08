package com.example.cbprofileutils.couchbase.service;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutateInSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ProfileDetailService {

    private static final Logger log = LoggerFactory.getLogger(ProfileDetailService.class);
    private final Cluster cluster;
    private final Bucket mainBucket;
    private final Bucket profileIdsBucket;
    private final Bucket tempBucket;

    public ProfileDetailService(
            CouchbaseTemplate couchbaseTemplate,
            @Value("${spring.data.couchbase.bucket-name}") String bucketName,
            @Value("${profileIdsBucket}") String profileIdsBucketName,
            @Value("${profileIdsTemporaryBucket}") String profileIdsTemporaryBucketName
    ) {
        this.cluster = couchbaseTemplate.getCouchbaseClientFactory().getCluster();
        this.mainBucket = cluster.bucket(bucketName);
        this.profileIdsBucket = cluster.bucket(profileIdsBucketName);
        this.tempBucket = cluster.bucket(profileIdsTemporaryBucketName);
    }

    public void addBIToProfileDetailsUsingBulkLoad(Map<String, JsonObject> biEntityMap) {
        biEntityMap.forEach((biMsisdn, json) -> {
            String msisdn = json.getString("MSISDN");
            if (msisdn == null || msisdn.length() < 3) return;

            String profileDetailId = msisdn.substring(2) + System.nanoTime();
            String profileKey = "p::" + profileDetailId;

            Collection profileIdsCollection = profileIdsBucket.defaultCollection();
            Collection tempCollection = tempBucket.defaultCollection();
            Collection mainCollection = mainBucket.defaultCollection();

            try {
                GetResult result = profileIdsCollection.get(msisdn);
                String existingId = result.contentAsObject().getString("id");
                updateExistingProfile(mainCollection, existingId, json);
            } catch (DocumentNotFoundException e) {
                createNewProfileDocuments(profileIdsCollection, tempCollection, mainCollection, biMsisdn, msisdn, profileKey, json);
            }
        });
    }

    private void updateExistingProfile(Collection mainCollection, String profileId, JsonObject biObject) {
        retryWithBackoff(3, () ->
                mainCollection.mutateIn(
                        profileId,
                        List.of(MutateInSpec.upsert("attrGrps.external_metrics", biObject).createPath())
                )
        );
    }

    private void createNewProfileDocuments(Collection profileIds, Collection temp, Collection main,
                                           String biMsisdn, String msisdn, String profileKey, JsonObject biObject) {

        JsonObject referenceDoc = JsonObject.create().put("id", profileKey);
        JsonObject newProfileDoc = JsonObject.create()
                .put("id", "p::" + biMsisdn)
                .put("name", "FAA_" + msisdn)
                .put("type", "FAA")
                .put("attrGrps", JsonObject.create().put("external_metrics", biObject));

        retryWithBackoff(3, () -> {
            profileIds.insert(msisdn, referenceDoc);
            temp.insert(msisdn, referenceDoc);
            main.insert(profileKey, newProfileDoc);
        });
    }

    private void retryWithBackoff(int maxAttempts, Runnable task) {
        int attemptsLeft = maxAttempts;
        long delay = 300;

        while (attemptsLeft-- > 0) {
            try {
                task.run();
                return;
            } catch (TimeoutException e) {
                try {
                    Thread.sleep(delay);
                    delay *= 2; // exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return;
            }
        }
    }
}
