package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonObject;
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
    public static Boolean filesOver = false;
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
        String filePath = args.length > 1 ? args[1] : applicationContext.getEnvironment().getProperty("biFilePath");
        BATCH_SIZE = Integer.parseInt(Objects.requireNonNull(applicationContext.getEnvironment().getProperty("batchSize")));
        THREAD_COUNT = Integer.parseInt(Objects.requireNonNull(applicationContext.getEnvironment().getProperty("threadCount")));

        logger.info("Application started");
        logger.info("filePath: {}", filePath);
        logger.info("batchSize: {}", BATCH_SIZE);
        logger.info("threadCount: {}", THREAD_COUNT);

        try {
            readAndUpdateDataFromDirectory(filePath);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void readAndUpdateDataFromDirectory(String filePath) throws IOException {
        startTime = System.currentTimeMillis();
        startDateTime = LocalDateTime.now();
        try {
//        Files.list(Paths.get(filePath))
//                .filter(Files::isRegularFile)
//                .forEach(filePath1 -> {
//                    try {
//                        readAndUpdateDataAsync(filePath1.toString());
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
            readAndUpdateDataAsync(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        filesOver = true;
    }

    public static void readAndUpdateDataAsync(String filePath) throws IOException {
        linesOver = false;
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        logger.info("file {} reading process has been started", filePath);
        String line = reader.readLine();
        Map<String, JsonObject> biEntityMap = new HashMap<>();
        int j = 0;

        Runnable task = () -> logger.info("number of active worker::{}", Controller.countActiveWorkerThreads());

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        executorService.scheduleAtFixedRate(task, 0, 1, TimeUnit.MINUTES);
        String id;
        while ((line = reader.readLine()) != null) {
            j++;
            String[] parts = line.split("\\|");
            JsonObject jsonObject = JsonObject.create()
                    .put("MO_KEY", parts[0])
                    .put("MSISDN", parts[1])
                    .put("IMEI", parts[2])
                    .put("PKG_TO_TOTAL_VOICE", parts[3])
                    .put("PKG_TO_TOTAL_DATA", parts[4])
                    .put("ROAM_INT_REV_TO_ARPU", parts[5])
                    .put("VAS_COST_TO_ARPU", parts[6])
                    .put("PAYG_DATA_REV_TO_ARPU", parts[7])
                    .put("ONNET_TO_OFFNET_REV", parts[8])
                    .put("ARPU", parts[9])
                    .put("IS_MULTI_SIM_USER", parts[10])
                    .put("SEGMENT", parts[11])
                    .put("AVG_ONNET_VOICE_MIN", parts[12])
                    .put("AVG_OFFNET_VOICE_MIN", parts[13])
                    .put("AVG_SMS_CNT", parts[14])
                    .put("AVG_DATA_USAGE", parts[15])
                    .put("SEGMENT2", parts[16]);
            id = jsonObject.getString("MSISDN").substring(2) + RandomUtil.getUnixTimeString().substring(5);
            biEntityMap.put(id, jsonObject);

            if (biEntityMap.size() % BATCH_SIZE == 0) {
                submitBatch(biEntityMap, j);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                biEntityMap = new HashMap<>();

            }
        }
        reader.close();
        logger.info("file {} reading process has ended", filePath);
        logger.info("last line number: {}", j);
        if (!biEntityMap.isEmpty()) {
            logger.info("last thread (Worker-{}) is creating...", j);
            submitBatch(biEntityMap, j);
        }
        linesOver = true;
    }

    private static void submitBatch(Map<String, JsonObject> biEntityMap, int j) {
        Thread workerThread = new Thread(new Worker(profileDetailService, biEntityMap), "Worker-" + j);
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