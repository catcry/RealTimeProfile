package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.service.ProfileDetailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

@Component
public class CbProfileUtilsApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(CbProfileUtilsApplication.class);
    private static final int THREAD_COUNT = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);

    private final org.springframework.context.ApplicationContext ctx;
    private final ProfileDetailService profileDetailService;

    @Value("${bi.file.path}")
    private String dataFilePath;
    @Value("${bi.data.separator}")
    private String biDataSeparator;
    @Value("${header.file.path}")
    private String headerFilePath;
    @Value("${bi.header.separator}")
    private String headerSeparator;

    @Value("${batchSize:5000}")
    private int batchSize;

    public CbProfileUtilsApplication(org.springframework.context.ApplicationContext ctx, ProfileDetailService profileDetailService) {
        this.ctx = ctx;
        this.profileDetailService = profileDetailService;
    }

    @Override
    public void run(String... args) throws Exception {
        long startTime = System.nanoTime();

        // load headers once
        List<String> headers;
        try (BufferedReader headerReader = Files.newBufferedReader(Paths.get(headerFilePath), StandardCharsets.UTF_8)) {
            String headerLine = headerReader.readLine();
            if (headerLine == null) {
                log.error("Header file is empty.");
                return;
            }
            headers = Arrays.asList(headerLine.split(headerSeparator, -1));
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT, r -> {
            Thread t = new Thread(r);
            t.setName("bi-consumer-" + t.getId());
//            t.setDaemon(true);
            return t;
        });
        BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(THREAD_COUNT * 2);
        CountDownLatch producerDone = new CountDownLatch(1);

        // Producer: stream CSV into batches
        Thread producer = new Thread(() -> {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(dataFilePath), StandardCharsets.UTF_8)) {
                String line;
                List<String> batch = new ArrayList<>(batchSize);
                reader.readLine(); // skip data header
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        batch.add(line);
                        if (batch.size() == batchSize) {
                            queue.put(new ArrayList<>(batch));
                            batch.clear();
                        }
                    }
                }
                if (!batch.isEmpty()) queue.put(batch);
            } catch (Exception e) {
                log.error("Error reading the data file", e);
            } finally {
                producerDone.countDown();
            }
        }, "bi-producer");

        producer.start();

        // Consumer task
        Runnable consumerTask = () -> {
            try {
                while (true) {
                    List<String> batch = queue.poll(1, TimeUnit.SECONDS);
                    if (batch != null) {
                        Map<String, JsonObject> biEntityMap = new HashMap<>(batch.size()*2);

                        for (String row : batch) {
                            try {
                                String[] values = row.split(biDataSeparator, -1);
                                JsonObject json = JsonObject.create();
                                if (values.length < headers.size()) {
                                    log.warn("Row has fewer columns than header, skipping: {}", row);
                                    continue;
                                }
                                for (int i = 0; i < headers.size() && i < values.length; i++) {
                                    json.put(headers.get(i), values[i]);
                                }
                                String msisdn = json.getString("MSISDN");
                                if (msisdn != null && !msisdn.isBlank()) {
                                    biEntityMap.put(msisdn, json);
                                }
                            } catch (Exception ex) {
                                log.warn("Skipping invalid CSV line: {}", row, ex);
                            }
                        }

                        if (!biEntityMap.isEmpty()) {
                            profileDetailService.addBIToProfileDetailsUsingBulkLoad(biEntityMap);
                        }
                    } else if (producerDone.getCount() == 0 && queue.isEmpty()) {
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Error processing batch", e);
            }
        };

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) futures.add(executor.submit(consumerTask));
        for (Future<?> f : futures) f.get();

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.nanoTime();
        log.info("Processing completed in {} seconds", (endTime - startTime) / 1_000_000_000);
        // Tell Spring to shut down cleanly; returns exit code (default 0)
        int code = org.springframework.boot.SpringApplication.exit(ctx);
        System.exit(code); // optional; remove in dev
    }
}
