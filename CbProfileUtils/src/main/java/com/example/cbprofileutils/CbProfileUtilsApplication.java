package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.service.ProfileDetailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class CbProfileUtilsApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(CbProfileUtilsApplication.class);

    private static final int THREAD_COUNT = Math.min(40, Math.max(4, Runtime.getRuntime().availableProcessors()));
    private static final int QUEUE_CAPACITY = THREAD_COUNT * 1024;

    private final ApplicationContext ctx;
    private final ProfileDetailService profileDetailService;

    @Value("${bi.file.path}") private String dataFilePath;
    @Value("${bi.data.separator}") private String biDataSeparator;
    @Value("${header.file.path}") private String headerFilePath;
    @Value("${batchSize:50000}") private int batchSize;

    public CbProfileUtilsApplication(ApplicationContext ctx, ProfileDetailService profileDetailService) {
        this.ctx = ctx;
        this.profileDetailService = profileDetailService;
    }

    private static String[] fastSplit(String s, char delim, int expectedCols) {
        String[] out = new String[expectedCols];
        int start = 0, idx = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == delim) {
                out[idx++] = s.substring(start, i);
                start = i + 1;
                if (idx >= expectedCols) break;
            }
        }
        if (idx < expectedCols) out[idx++] = s.substring(start);
        return idx == out.length ? out : Arrays.copyOf(out, idx);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting profile data processing...");

        if (!Files.exists(Paths.get(dataFilePath)) || !Files.exists(Paths.get(headerFilePath))) {
            log.error("Data or header file not found.");
            System.exit(1);
        }

        long startTime = System.nanoTime();

        // --- Read headers ---
        final List<String> headers;
        final int msisdnIdx;
        try (BufferedReader headerReader = Files.newBufferedReader(Paths.get(headerFilePath), StandardCharsets.UTF_8)) {
            String headerLine = headerReader.readLine();
            if (headerLine == null) {
                log.error("Header file is empty.");
                System.exit(1);
            }
            headers = Arrays.asList(headerLine.split(",", -1));
            msisdnIdx = headers.indexOf("MSISDN");
            if (msisdnIdx < 0) {
                log.error("MSISDN header not found");
                System.exit(1);
            }
        }

        // --- Producer-consumer setup ---
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        CountDownLatch producerDone = new CountDownLatch(1);
        AtomicInteger totalProcessed = new AtomicInteger(0);
        AtomicInteger totalSkipped = new AtomicInteger(0);

        // --- Producer thread ---
        Thread producer = new Thread(() -> {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(dataFilePath), StandardCharsets.UTF_8)) {
                String line;
                long recordsRead = 0;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank()) {
                        queue.put(line);
                        if (++recordsRead % 100_000 == 0) log.info("Read {} records", recordsRead);
                    }
                }
            } catch (Exception e) {
                log.error("Error reading data file", e);
            } finally {
                producerDone.countDown();
            }
        }, "bi-producer");
        producer.start();

        // --- Consumers ---
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        Runnable consumerTask = () -> {
            List<String> localBatch = new ArrayList<>(batchSize);
            while (true) {
                localBatch.clear();
                queue.drainTo(localBatch, batchSize);

                // poll if nothing drained
                if (localBatch.isEmpty() && producerDone.getCount() > 0) {
                    try {
                        String line = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (line != null) localBatch.add(line);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                if (localBatch.isEmpty() && producerDone.getCount() == 0 && queue.isEmpty()) break;

                if (!localBatch.isEmpty()) {
                    Map<String, JsonObject> biEntityMap = new HashMap<>();
                    for (String row : localBatch) {
                        try {
                            String[] values = fastSplit(row, biDataSeparator.charAt(0), headers.size());
                            if (values.length <= msisdnIdx || values[msisdnIdx].isBlank()) {
                                totalSkipped.incrementAndGet();
                                continue;
                            }
                            if (values.length < headers.size()) values = Arrays.copyOf(values, headers.size());
                            JsonObject json = JsonObject.create();
                            for (int i = 0; i < headers.size(); i++) json.put(headers.get(i), values[i]);
                            biEntityMap.put(values[msisdnIdx], json);
                        } catch (Exception e) {
                            totalSkipped.incrementAndGet();
                        }
                    }
                    if (!biEntityMap.isEmpty()) {
                        try {
                            profileDetailService.addBIToProfileDetailsUsingBulkLoad(biEntityMap).join();
                        } catch (Exception e) {
                            log.error("Error processing BI entity map", e);
                        }
                        int processed = totalProcessed.addAndGet(biEntityMap.size());
                        if (processed % 100_000 == 0) log.info("Processed {}, skipped {}", processed, totalSkipped.get());
                    }
                }
            }
        };

        // --- Submit consumers ---
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) futures.add(executor.submit(consumerTask));
        for (Future<?> f : futures) f.get();

        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.MINUTES)) log.warn("Executor did not terminate gracefully");

        double duration = (System.nanoTime() - startTime) / 1_000_000_000.0;
        log.info("=========================================");
        log.info("PROCESSING COMPLETED");
        log.info("Total time: {} s, processed: {}, skipped: {}, RPS: {}",
                String.format("%.2f", duration), totalProcessed.get(), totalSkipped.get(),
                String.format("%.2f", totalProcessed.get() / duration));

        int code = org.springframework.boot.SpringApplication.exit(ctx);
        System.exit(code);
    }
}
