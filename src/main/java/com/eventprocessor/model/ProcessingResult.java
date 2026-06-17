package main.java.com.eventprocessor.model;

public record ProcessingResult(
        String eventId,
        int processorId,
        long durationMs,
        boolean success
) {}
