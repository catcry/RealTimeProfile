package com.bonyan.tps;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Config {
    private final Properties props = new Properties();

    public Config(String path) {
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config from: " + path, e);
        }
    }

    public List<String> hosts() {
        String csv = props.getProperty("couchbase.hosts", "").trim();
        return Arrays.asList(csv.split("\\s*,\\s*"));
    }

    public String bucket() { return props.getProperty("couchbase.bucket"); }
    public String user()   { return props.getProperty("couchbase.username"); }
    public String pass()   { return props.getProperty("couchbase.password"); }

    public String msisdnFile() { return props.getProperty("input.msisdn.file"); }

    // Defaults now; tunable later
    public int threads()  { return Integer.parseInt(props.getProperty("benchmark.threads", "8")); }
    public int duration() { return Integer.parseInt(props.getProperty("benchmark.duration.seconds", "60")); }

    public boolean sslEnabled() {
        return Boolean.parseBoolean(props.getProperty("ssl.enabled", "false"));
    }
    public String truststorePath() { return props.getProperty("ssl.truststore.path"); }
    public String truststorePassword() { return props.getProperty("ssl.truststore.password"); }

}
