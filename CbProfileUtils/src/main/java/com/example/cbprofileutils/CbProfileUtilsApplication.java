package com.example.cbprofileutils;

import com.couchbase.client.java.json.JsonObject;
import com.example.cbprofileutils.couchbase.repository.CbProfileDetailRepository;
import com.example.cbprofileutils.couchbase.repository.CbProfileRepository;
import com.example.cbprofileutils.couchbase.service.ProfileDetailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan(basePackages = {
        "com.example.cbprofileutils",
        "com.example.cbprofileutils.couchbase.config",
        "com.example.cbprofileutils.couchbase.entity",
        "com.example.cbprofileutils.couchbase.repository",
        "com.example.cbprofileutils.couchbase.service"
})
public class CbProfileUtilsApplication {

    private static final Logger logger = LoggerFactory.getLogger(CbProfileUtilsApplication.class);
    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static CbProfileDetailRepository cbProfileDetailRepository;
    public static CbProfileRepository cbProfileRepository;
    public static Boolean filesOver = false;
    public static Boolean linesOver;
    public static Long startTime;
    public static LocalDateTime startDateTime;
    private static ProfileDetailService profileDetailService;
    private static int BATCH_SIZE;
    private static int THREAD_COUNT;

    public static void main(String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(CbProfileUtilsApplication.class, args);

        cbProfileDetailRepository = applicationContext.getBean(CbProfileDetailRepository.class);
        cbProfileRepository = applicationContext.getBean(CbProfileRepository.class);
        profileDetailService = applicationContext.getBean(ProfileDetailService.class);
        String filePath = args.length > 1 ? args[1] : applicationContext.getEnvironment().getProperty("biFilePath");
        String headerFilePath = args.length > 2 ? args[2] : applicationContext.getEnvironment().getProperty("headerFilePath");
        BATCH_SIZE = Integer.parseInt(Objects.requireNonNull(applicationContext.getEnvironment().getProperty("batchSize")));
        THREAD_COUNT = Integer.parseInt(Objects.requireNonNull(applicationContext.getEnvironment().getProperty("threadCount")));

        logger.info("Application started");
        logger.info("filePath: {}", filePath);
        logger.info("batchSize: {}", BATCH_SIZE);
        logger.info("threadCount: {}", THREAD_COUNT);

        try {
            readAndUpdateDataFromDirectory(filePath, headerFilePath);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void readAndUpdateDataFromDirectory(String filePath, String headerPath) throws IOException {
        startTime = System.currentTimeMillis();
        startDateTime = LocalDateTime.now();
        try {
            readAndUpdateDataAsync(filePath, headerPath);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        filesOver = true;
    }

    public static void readAndUpdateDataAsync(String filePath, String headerPath) throws IOException {
        linesOver = false;
        String[] headers;
        try (BufferedReader headerReader = new BufferedReader(new FileReader(headerPath));
             BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            logger.info("file {} reading process has been started", filePath);
            String headerLine = headerReader.readLine();
            headers = headerLine.split(",");
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null && lineNumber < 10) {
                lineNumber++;
                String[] parts = line.split("\\|");
                if (parts.length != headers.length) {
                    handleInvalidLine(lineNumber, line);
                    continue;
                }
                JsonObject jsonObject = JsonObject.create();
                for (int i = 0; i < headers.length; i++) {
                    jsonObject.put(headers[i], parts[i]);
                }
                profileDetailService.updateRtpBucket(jsonObject);
            }
        }

    }

    public static void handleInvalidLine(int lineNumber, String line) {
        logger.error("Invalid data at line {}: {}", lineNumber, line);
        String directoryPath = "errors";  // Change to your directory path
        String fileName = "errors.txt";
        boolean directoryExists = true;
        boolean fileExists = true;
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directoryExists = directory.mkdirs();
        }
        if (directoryExists) {
            File file = new File(directoryPath + File.separator + fileName);
            if (!file.exists()) {
                try {
                    fileExists = file.createNewFile();
                } catch (IOException e) {
                    logger.error("error while crating invalid lines error file: {}", e.getMessage());
                    return;
                }
            }
            if (fileExists) {
                try {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
                        writer.write(line);
                        writer.newLine();
                    }
                } catch (IOException e) {
                    logger.error("error while writing invalid line at error file: {}", e.getMessage());
                }
            }
        }
    }
}