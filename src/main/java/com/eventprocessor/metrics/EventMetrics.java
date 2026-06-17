package com.eventprocessor.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EventMetrics {

    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);
    private final AtomicLong totalDurationMs = new AtomicLong(0);

    public void recordSuccess(long durationMs) {
        totalProcessed.incrementAndGet();
        totalDurationMs.addAndGet(durationMs);
    }

    public void recordFailure() {
        totalFailed.incrementAndGet();
    }

    public void printSummary() {
        int processed = totalProcessed.get();
        double avg = processed == 0 ? 0 : (double) totalDurationMs.get() / processed;
        System.out.printf("%nMetrics Summary:%n");
        System.out.printf("  Processed : %d%n", processed);
        System.out.printf("  Failed    : %d%n", totalFailed.get());
        System.out.printf("  Avg time  : %.1fms%n", avg);
    }
}
