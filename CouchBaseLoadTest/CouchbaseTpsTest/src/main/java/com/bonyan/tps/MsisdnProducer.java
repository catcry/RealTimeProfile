package com.bonyan.tps;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.BlockingQueue;

public class MsisdnProducer implements Runnable {
    private final String path;
    private final BlockingQueue<String> queue;
    private volatile boolean stopped = false;

    public MsisdnProducer(String path, BlockingQueue<String> queue) {
        this.path = path;
        this.queue = queue;
    }

    public void stop() { stopped = true; }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while (!stopped && (line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String key = line.startsWith("MSISDN::") ? line : "MSISDN::" + line;
                queue.put(key);
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e);
        }
    }
}
