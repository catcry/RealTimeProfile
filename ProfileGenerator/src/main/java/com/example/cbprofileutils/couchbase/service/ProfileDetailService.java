package com.example.cbprofileutils.couchbase.service;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.example.cbprofileutils.couchbase.entity.CbBIEntity;
import com.example.cbprofileutils.couchbase.entity.CbProfileDetailEntity;
import com.example.cbprofileutils.couchbase.entity.CbProfileEntity;
import com.example.cbprofileutils.couchbase.entity.CbRtdEntity;
import com.example.cbprofileutils.couchbase.repository.ProfileDetailRepository;
import com.example.cbprofileutils.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
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

    public void insertIntoBuckets(Map<String, JsonObject> profileHashMap, Map<String, JsonObject> profileDetailHashMap) {
        try {
            for (Map.Entry<String, JsonObject> profileDocument : profileHashMap.entrySet()) {
                couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(bucketName)
                        .defaultCollection().insert(profileDocument.getKey(), profileDocument.getValue());
            }
            for (Map.Entry<String, JsonObject> profileDetailDocument : profileDetailHashMap.entrySet()) {
                JsonObject jsonId = JsonObject.create().put("id", profileDetailDocument.getKey());
                String msisdn = profileDetailDocument.getValue().getString("name").substring(3);
                couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(profileIdsBucket)
                        .defaultCollection().insert(msisdn, jsonId);
                couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(profileIdsTemporaryBucket)
                        .defaultCollection().insert(msisdn, jsonId);
                couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(bucketName)
                        .defaultCollection().insert(profileDetailDocument.getKey(), profileDetailDocument.getValue());
                logger.info("\ninsert rtp bucket id: {}", profileDetailDocument.getKey());
                logger.info("\ninsert msisdn: {}", msisdn);
            }

        } catch (TimeoutException e) {
            logger.info("\ninsert timeoutException: {}", e.getMessage());
            insertIntoBuckets(profileHashMap, profileDetailHashMap);
        }
    }
}
