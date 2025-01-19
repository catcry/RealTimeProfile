package com.example.cbprofileutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

public class Controller implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);


    private final ThreadController controller;
    private final int threadCount;
    private volatile boolean running = true;

    public Controller(ThreadController controller, int threadCount) {
        this.controller = controller;
        this.threadCount = threadCount;
    }

    public static int countActiveWorkerThreads() {
        Map<Thread, StackTraceElement[]> allThreads = Thread.getAllStackTraces();
        long count = allThreads.keySet().stream()
                .filter(thread -> thread.getName().startsWith("Worker-") && thread.isAlive())
                .count();
        return (int) count;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            if (countActiveWorkerThreads() > this.threadCount) {
                controller.setShouldSleep(true);
            } else if (CbProfileUtilsApplication.linesOver
                    && countActiveWorkerThreads() == 0) {
                try {
                    Thread.sleep(5000);
                    if (countActiveWorkerThreads() == 0) {
                        LocalDateTime startDate = CbProfileUtilsApplication.startDateTime;
                        LocalDateTime endDate = LocalDateTime.now();
                        logger.info("start date time: {}", startDate);
                        logger.info("end date time: {}", endDate);

                        // Parse the two LocalDateTime instances
                        Duration duration = Duration.between(startDate, endDate);

                        long totalSeconds = duration.getSeconds();
                        logger.info("total time: {} Hours, {} Minutes, {} Seconds, {} Milli Seconds", totalSeconds / 3600,
                                (totalSeconds % 3600) / 60, totalSeconds % 60,
                                (System.currentTimeMillis() - CbProfileUtilsApplication.startTime) % 1000);
                        stop();
                        System.exit(0);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    Thread.sleep(2000);
                    if (countActiveWorkerThreads() < this.threadCount) {
                        controller.setShouldSleep(false);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
