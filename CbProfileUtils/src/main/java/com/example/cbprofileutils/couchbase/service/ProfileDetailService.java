package com.example.cbprofileutils.couchbase.service;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.example.cbprofileutils.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class ProfileDetailService {

    private static final Logger logger = LoggerFactory.getLogger(ProfileDetailService.class);

    private final CouchbaseTemplate couchbaseTemplate;
    @Value("${spring.data.couchbase.bucket-name}")
    private String bucketName;
    @Value("${profileIdsBucket}")
    private String profileIdsBucket;
    @Value("${profileIdsTemporaryBucket}")
    private String profileIdsTemporaryBucket;


    public ProfileDetailService(CouchbaseTemplate couchbaseTemplate) {
        this.couchbaseTemplate = couchbaseTemplate;
    }

    public void addBIToProfileDetailsUsingBulkLoad(Map<String, JsonObject> biEntityMap) {
        Set<String> biMsisdnSet = biEntityMap.keySet();
        for (String biMsisdn : biMsisdnSet) {
            JsonObject jsonObject = biEntityMap.get(biMsisdn);
            String msisdn = jsonObject.getString("MSISDN");
            String profileDetailId = msisdn.substring(2) + RandomUtil.getUnixTimeString().substring(5);

            try {
                GetResult getResult = couchbaseTemplate.getCouchbaseClientFactory().getCluster()
                        .bucket(profileIdsBucket).defaultCollection().get(msisdn);
                updateRtpBucket(getResult.contentAsObject().get("id").toString(), biEntityMap.get(biMsisdn));
            } catch (DocumentNotFoundException e){
                JsonObject newDocument = JsonObject.create()
                        .put("id", "p::" + biMsisdn)
                        .put("name", "FAA_" + msisdn)
                        .put("type", "FAA")
                        .put("attrGrps", JsonObject.create().put("BI", jsonObject));
                JsonObject jsonId = JsonObject.create().put("id", "p::" + profileDetailId);
                insertIntoBuckets(newDocument, jsonId, profileDetailId, msisdn);
            }
        }
    }

    private void updateRtpBucket(String rtpBucketId, JsonObject biObject) {
        try {
            couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(bucketName)
                    .defaultCollection()
                    .mutateIn(rtpBucketId, List.of(
                            MutateInSpec.upsert("attrGrps.BI", biObject)
                                    .createPath()
                    ));
        } catch (TimeoutException e) {
            logger.info("\nupdate timeoutException: {}", e.getMessage());
            logger.info("\nupdate timeoutException for rtp bucket id: p::{}", rtpBucketId);
            updateRtpBucket(rtpBucketId, biObject);
        }
    }

    private void insertIntoBuckets(JsonObject newDocument, JsonObject jsonId, String profileDetailId, String msisdn) {
        try {
            couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(profileIdsBucket)
                    .defaultCollection().insert(msisdn, jsonId);
            couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(profileIdsTemporaryBucket)
                    .defaultCollection().insert(msisdn, jsonId);
            couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(bucketName)
                    .defaultCollection().insert("p::" + profileDetailId, newDocument);
        } catch (TimeoutException e) {
            logger.info("\ninsert timeoutException: {}", e.getMessage());
            logger.info("\ninsert timeoutException for rtp bucket id: p::{}", profileIdsBucket);
            logger.info("\ninsert timeoutException for map msisdn: {}", msisdn);
            insertIntoBuckets(newDocument, jsonId, profileDetailId, msisdn);
        }
    }
}
