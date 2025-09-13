package com.bonyan.tps;

import com.couchbase.client.java.Bucket;

public class Main {
    public static void main(String[] args) throws Exception {
        String configPath = null;
        String singleKey  = null;

        Integer threads = null;
        Integer duration = null;
        String file = null;
        Double rate = null;

        for (String a : args) {
            if (a.startsWith("--config="))   configPath = a.substring("--config=".length());
            else if (a.startsWith("--key=")) singleKey  = a.substring("--key=".length());
            else if (a.startsWith("--threads="))  threads  = Integer.parseInt(a.substring("--threads=".length()));
            else if (a.startsWith("--duration=")) duration = Integer.parseInt(a.substring("--duration=".length()));
            else if (a.startsWith("--file="))     file     = a.substring("--file=".length());
            else if (a.startsWith("--rate="))     rate     = Double.parseDouble(a.substring("--rate=".length()));
        }

        if (configPath == null) {
            System.err.println("Usage:");
            System.err.println("  java -jar app.jar --config=PATH [--key=MSISDN::<num>]");
            System.err.println("  java -jar app.jar --config=PATH --file=PATH [--threads=N] [--duration=SEC] [--rate=OPS_PER_SEC]");
            System.exit(2);
        }

        Config cfg = new Config(configPath);
        if (threads == null)  threads = cfg.threads();
        if (duration == null) duration = cfg.duration();
        if (file == null)     file = cfg.msisdnFile();

        CbClientFactory factory = new CbClientFactory(cfg);
        try {
            Bucket bucket = factory.open();
            System.out.println("Connected to bucket: " + cfg.bucket());

            if (singleKey != null) {
                SmokeTest.run(bucket, singleKey);
                return;
            }

            if (file == null) {
                System.err.println("No --key and no --file provided. Nothing to do.");
                return;
            }

            TpsRunner.Options opts = new TpsRunner.Options(
                    threads, duration, 50_000, (rate != null && rate > 0) ? rate : null
            );
            TpsRunner.run(bucket, file, opts);

        } finally {
            factory.close();
        }
    }
}
