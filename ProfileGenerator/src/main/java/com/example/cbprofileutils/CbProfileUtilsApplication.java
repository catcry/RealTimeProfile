package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.entity.CbProfileDetailEntity;
import com.example.cbprofileutils.couchbase.entity.CbProfileEntity;
import com.example.cbprofileutils.couchbase.entity.CbRtdEntity;
import com.example.cbprofileutils.couchbase.repository.ProfileDetailRepository;
import com.example.cbprofileutils.couchbase.repository.ProfileRepository;
import com.example.cbprofileutils.couchbase.service.ProfileDetailService;
import com.example.cbprofileutils.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableTransactionManagement
@ComponentScan(basePackages = {
        "com.example.cbprofileutils",
        "com.example.cbprofileutils.couchbase.config",
        "com.example.cbprofileutils.couchbase.repository",
        "com.example.cbprofileutils.couchbase.service"
})
public class CbProfileUtilsApplication {

    private static final Logger logger = LoggerFactory.getLogger(CbProfileUtilsApplication.class);
    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static ProfileDetailService profileDetailService;
    public static ProfileDetailRepository profileDetailRepository;
    public static ProfileRepository profileRepository;
    public static Boolean linesOver;
    public static Long startTime;
    public static LocalDateTime startDateTime;
    private static int BATCH_SIZE;
    private static int THREAD_COUNT;

    public static void main(String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(CbProfileUtilsApplication.class, args);

        profileDetailRepository = applicationContext.getBean(ProfileDetailRepository.class);
        profileRepository = applicationContext.getBean(ProfileRepository.class);
        profileDetailService = applicationContext.getBean(ProfileDetailService.class);
        BATCH_SIZE = 500;
        THREAD_COUNT = 20;

        logger.info("Application started");
        logger.info("batchSize: {}", BATCH_SIZE);
        logger.info("threadCount: {}", THREAD_COUNT);

        generate();
    }

    public static void generate(){
        startTime = System.currentTimeMillis();
        startDateTime = LocalDateTime.now();
        generateReadyProfiles();
    }

    public static void generateReadyProfiles() {
        linesOver = false;
        Runnable task = () -> logger.info("number of active worker::{}", Controller.countActiveWorkerThreads());

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        executorService.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);

        Map<String, JsonObject> profileHashMap = new HashMap<>();
        Map<String, JsonObject> profileDetailHashMap = new HashMap<>();
        int j = 1;
        for (int i = 20000000; i <= 30000000; i++) {
            String msisdn = "9891" + i;
            String profileId = addProfile(profileHashMap, msisdn);
            addProfileDetail(profileDetailHashMap, profileId, msisdn);
            if (profileHashMap.size() >= BATCH_SIZE) {
                submitBatch(profileHashMap, profileDetailHashMap, j++);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                profileHashMap = new HashMap<>();
                profileDetailHashMap = new HashMap<>();

            }
        }

        if (!profileDetailHashMap.isEmpty()) {
            logger.info("last thread (Worker-{}) is creating...", j);
            submitBatch(profileHashMap, profileDetailHashMap, j);
        }
        linesOver = true;
    }
    public static String addProfile(Map<String, JsonObject> profileHashMap, String callingNumber) {
        JsonObject profile = JsonObject.create();
        profile.put("type", "MSISDN");
        profile.put("value", callingNumber);

        String profileDetailId = RandomUtil.getUnixTimeString();
        JsonObject profileInfo = JsonObject.create();
        profileInfo.put("id", profileDetailId);
        profileInfo.put("priority",1);
        profileInfo.put("attr", JsonArray.create());
        profileInfo.put("roles", JsonArray.create().add("Owner"));

        JsonArray profileDetails = JsonArray.create();
        profileDetails.add(profileInfo);

        profile.put("profiles",profileDetails);
        profileHashMap.put(CbProfileEntity.PROFILE_ID_PREFIX + callingNumber, profile);
        return profileDetailId;
    }


    public static void addProfileDetail(Map<String, JsonObject> profileDetailHashMap, String profileId, String msisdn) {
        JsonObject profileDetail = JsonObject.create();
        profileDetail.put("id", profileId);
        profileDetail.put("name", "FAA_" + msisdn);
        profileDetail.put("type","FAA");
        JsonObject attrGroup = JsonObject.create();
        attrGroup.put("rtd", CbRtdEntity.getRandomJsonRtd());
        profileDetail.put("attrGrps", attrGroup);
        profileDetailHashMap.put(CbProfileDetailEntity.PROFILE_DETAIL_ID_PREFIX + profileId, profileDetail);
    }

    private static void submitBatch(Map<String, JsonObject> profileHashMap,
                                    Map<String, JsonObject> profileDetailHashMap, int j) {
        Thread workerThread = new Thread(
                new Worker(profileDetailService, profileHashMap, profileDetailHashMap), "Worker-" + j);
        workerThread.start();
        logger.info("thread {} started", workerThread.getName());
        ThreadController controller = new ThreadController();
        Controller controller1 = new Controller(controller, THREAD_COUNT);
        Thread controllerThread = new Thread(controller1, "WorkerCountController");
        if (Controller.countActiveWorkerThreads() > 0) {
            controllerThread.setPriority(10);
            controllerThread.start();
            logger.info("thread {} started", controllerThread.getName());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        while (controller.shouldSleep()) {
            logger.info("Main thread is sleeping for 3 seconds");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.info("Main thread is continuing the program");
    }
}