package com.eventprocessor.metrics;

import java.util.concurrent.ConcurrentHashMap;

public class EventTypeCounter {

    private final ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();

    public void record(String eventType) {
        counts.merge(eventType, 1, Integer::sum);
    }

    public void printCounts() {
        System.out.printf("%nEvent Type Counts:%n");
        counts.forEach((type, count) ->
            System.out.printf("  %-20s: %d%n", type, count)
        );
    }
}
