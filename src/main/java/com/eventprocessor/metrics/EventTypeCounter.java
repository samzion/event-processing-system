package com.eventprocessor.metrics;

import java.util.concurrent.ConcurrentHashMap;

public class EventTypeCounter {

    private final ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> lastPayload = new ConcurrentHashMap<>();


    public void record(String eventType, String payload) {

        counts.merge(eventType, 1, Integer::sum);
        lastPayload.put(eventType, payload);

    }

    public void printCounts() {
        System.out.printf("%nEvent Type Counts:%n");
        counts.forEach((type, count) ->
                System.out.printf("  %-20s: count=%-5d last=%s%n",
                        type, count, lastPayload.getOrDefault(type, "N/A"))
        );
    }
}
