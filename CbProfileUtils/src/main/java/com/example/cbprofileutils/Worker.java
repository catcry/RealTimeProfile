package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.service.ProfileDetailService;

import java.util.Map;

public class Worker implements Runnable {
    private final ProfileDetailService profileDetailService;
    private final Map<String, JsonObject> biEntityHashMap;

    public Worker(ProfileDetailService profileDetailService,
                  Map<String, JsonObject> biEntityHashMap1) {
        this.profileDetailService = profileDetailService;
        this.biEntityHashMap = biEntityHashMap1;
    }

    @Override
    public void run() {
        profileDetailService.addBIToProfileDetailsUsingBulkLoad(biEntityHashMap);
    }
}
