package com.eventprocessor.eventbus;

public record DomainEvent(
        String eventType,
        String payload,
        long timestampMs
) {
    public DomainEvent(String eventType, String payload) {
        this(eventType, payload, System.currentTimeMillis());
    }
}