package com.example.cbprofileutils;

public class ThreadController {
    private volatile boolean shouldSleep = false;

    public boolean shouldSleep() {
        return shouldSleep;
    }

    public synchronized void setShouldSleep(boolean shouldSleep) {
        this.shouldSleep = shouldSleep;
    }
}
