package com.eventprocessor.model;

public class Event {

    public enum EventType {
        PRICE_UPDATE,
        TRADE_CONFIRMATION,
        ALERT
    }

    private final String id;
    private final EventType type;
    private final String payload;
    private final long timestampMs;

    public Event(String id, EventType type, String payload) {
        this.id = id;
        this.type = type;
        this.payload = payload;
        this.timestampMs = System.currentTimeMillis();
    }

    public String getId() { return id; }
    public EventType getType() { return type; }
    public String getPayload() { return payload; }
    public long getTimestampMs() { return timestampMs; }

    @Override
    public String toString() {
        return String.format("Event[id=%s, type=%s, payload=%s]", id, type, payload);
    }
}
