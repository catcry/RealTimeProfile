package com.example.cbprofileutils.couchbase.service;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.entity.PsqlProfileEntity;
import com.example.cbprofileutils.couchbase.repository.CbProfileDetailRepository;
import com.example.cbprofileutils.couchbase.repository.PsqlProfileRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class ProfileDetailService {

    private static final Logger logger = LoggerFactory.getLogger(ProfileDetailService.class);

    private final CbProfileDetailRepository cbProfileDetailRepository;
    private final PsqlProfileRepository psqlProfileRepository;

    public ProfileDetailService(CbProfileDetailRepository cbProfileDetailRepository, PsqlProfileRepository psqlProfileRepository) {
        this.cbProfileDetailRepository = cbProfileDetailRepository;
        this.psqlProfileRepository = psqlProfileRepository;
    }

    public void addBIToProfileDetailsUsingBulkLoad(Map<String, JsonObject> biEntityMap) {
        Set<String> biMsisdnSet = biEntityMap.keySet();
        for (String biMsisdn : biMsisdnSet) {
            JsonObject jsonObject = biEntityMap.get(biMsisdn);
            String msisdn = jsonObject.getString("MSISDN");
            try {
                cbProfileDetailRepository.updateProfileDetailByName(biEntityMap.get(biMsisdn), "FAA_" + msisdn);
            } catch (DocumentNotFoundException e){
                logger.info("There is no profile for this number '" + msisdn + "' in couchbase");
            }
        }
    }


    public void updateRtpBucket(JsonObject biObject) {
        String msisdn = biObject.getString("MSISDN");
        try {
            String name = "FAA_" + biObject.getString("MSISDN");
            List<PsqlProfileEntity> profileEntities = psqlProfileRepository.findByName(name);
            if (profileEntities != null && !profileEntities.isEmpty()) {
                PsqlProfileEntity psqlProfile = profileEntities.get(0);
                cbProfileDetailRepository.updateProfileDetail(biObject, "p::" + psqlProfile.getId());
            }
        } catch (Exception e) {
            logger.info("There is no profile for this number '" + msisdn + "' in couchbase, error: " + e.getMessage());
        }
    }

    public void updateRtpBucketByName(JsonObject biObject) {
        String msisdn = biObject.getString("MSISDN");
        try {
            String name = "FAA_" + biObject.getString("MSISDN");
            cbProfileDetailRepository.updateProfileDetailByName(biObject, name);
        } catch (Exception e) {
            logger.info("There is no profile for this number '" + msisdn + "' in couchbase");
        }
    }

}
