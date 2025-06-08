package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.service.ProfileDetailService;
import com.example.cbprofileutils.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

@Service
public class BiFileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(BiFileProcessor.class);

    private static final Map<String, JsonObject> POISON_PILL = Collections.emptyMap();

    private final ProfileDetailService profileDetailService;
    private final int batchSize;
    private final int threadCount;
    private final String headerSeparator;
    private final String dataSeparator;

    private final BlockingQueue<Map<String, JsonObject>> queue;
    private final ExecutorService consumers;

    public BiFileProcessor(ProfileDetailService profileDetailService, Environment env) {
        this.profileDetailService = profileDetailService;
        this.batchSize = Integer.parseInt(env.getProperty("batchSize", "500"));
        this.threadCount = Integer.parseInt(env.getProperty("threadCount", "100"));
        this.headerSeparator = env.getProperty("bi.header.separator", ",");
        this.dataSeparator = env.getProperty("bi.data.separator", ",");
        this.queue = new LinkedBlockingQueue<>();
        this.consumers = Executors.newFixedThreadPool(threadCount);
    }

    public void process(String dataPath, String headerPath) throws IOException {
        LocalDateTime startDateTime = LocalDateTime.now();

        String[] headers;
        try (BufferedReader headerReader = new BufferedReader(new FileReader(headerPath))) {
            headers = headerReader.readLine().split(headerSeparator);
        }

        // Start consumers
        for (int i = 0; i < threadCount; i++) {
            consumers.submit(this::consumeBatch);
        }

        try (BufferedReader dataReader = new BufferedReader(new FileReader(dataPath))) {
            Map<String, JsonObject> batch = new HashMap<>();
            String line;
            int lineNumber = 0;

            while ((line = dataReader.readLine()) != null && lineNumber++ < 1_000_000) {
                String[] values = line.split(dataSeparator);
                if (values.length != headers.length) {
                    writeInvalidLine(lineNumber, line);
                    continue;
                }

                JsonObject obj = createJsonObject(headers, values);
                String id = obj.getString("MSISDN").substring(2)+ RandomUtil.getUnixTimeString().substring(5); // Unique ID
                batch.put(id, obj);

                if (batch.size() >= batchSize) {
                    queue.put(new HashMap<>(batch));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                queue.put(batch);
            }

        } catch (Exception e) {
            logger.error("Error during file processing: {}", e.getMessage(), e);
        } finally {
            // Signal end of input
            for (int i = 0; i < threadCount; i++) {
                queue.offer(POISON_PILL);
            }

            consumers.shutdown();
            try {
                consumers.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for consumers to finish.");
                Thread.currentThread().interrupt();
            }

            Duration duration = Duration.between(startDateTime, LocalDateTime.now());
            long seconds = duration.getSeconds();
            logger.info("All done. Total time: {}h {}m {}s", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
            System.exit(0);
        }
    }

    private void consumeBatch() {
        try {
            while (true) {
                Map<String, JsonObject> batch = queue.take();
                if (batch == POISON_PILL) break;

                profileDetailService.addBIToProfileDetailsUsingBulkLoad(batch);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private JsonObject createJsonObject(String[] headers, String[] values) {
        JsonObject json = JsonObject.create();
        for (int i = 0; i < headers.length; i++) {
            json.put(headers[i], values[i]);
        }
        return json;
    }

    private void writeInvalidLine(int lineNumber, String line) {
        logger.warn("Invalid line at {}: {}", lineNumber, line);
        File errorFile = new File("errors/errors.txt");

        errorFile.getParentFile().mkdirs();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(errorFile, true))) {
            writer.write(line);
            writer.newLine();
        } catch (IOException e) {
            logger.error("Error writing invalid line: {}", e.getMessage());
        }
    }
}
