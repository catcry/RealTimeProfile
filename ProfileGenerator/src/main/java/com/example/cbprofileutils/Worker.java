package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.entity.*;
import com.example.cbprofileutils.couchbase.service.ProfileDetailService;
import com.example.cbprofileutils.util.RandomUtil;

import java.util.HashMap;
import java.util.Map;

public class Worker implements Runnable {
    private final ProfileDetailService profileDetailService;
    private final Map<String, JsonObject> profileHashMap;
    private final Map<String, JsonObject> profileDetailHashMap;

    public Worker(ProfileDetailService profileDetailService,
                  Map<String, JsonObject> profileHashMap, Map<String, JsonObject> profileDetailHashMap) {
        this.profileDetailService = profileDetailService;
        this.profileHashMap = profileHashMap;
        this.profileDetailHashMap = profileDetailHashMap;
    }

    @Override
    public void run() {
        profileDetailService.insertIntoBuckets(profileHashMap, profileDetailHashMap);
    }

}
