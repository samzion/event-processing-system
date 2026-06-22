package com.eventprocessor.kafka;

import com.eventprocessor.eventbus.DomainEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// Simulates a single Kafka topic with ordered, persistent storage
public class SimulatedKafkaTopic {

    private final String topicName;
    private final List<DomainEvent> log = new ArrayList<>(); // immutable append-only log
    private final Map<String, AtomicInteger> consumerOffsets = new ConcurrentHashMap<>();

    public SimulatedKafkaTopic(String topicName) {
        this.topicName = topicName;
    }

    // Producer: append to log — like Kafka producer.send()
    public synchronized int publish(DomainEvent event) {
        log.add(event);
        int offset = log.size() - 1;
        System.out.printf("[Kafka:%s] Published at offset %d: %s%n",
                topicName, offset, event.payload());
        return offset;
    }

    // Consumer: poll from a specific offset — like Kafka consumer.poll()
    public synchronized List<DomainEvent> poll(String consumerGroup, int maxEvents) {
        AtomicInteger offset = consumerOffsets.computeIfAbsent(
                consumerGroup, k -> new AtomicInteger(0)
        );

        int current = offset.get();
        if (current >= log.size()) return List.of(); // nothing new

        int end = Math.min(current + maxEvents, log.size());
        List<DomainEvent> batch = new ArrayList<>(log.subList(current, end));
        offset.set(end); // commit offset
        return batch;
    }

    // Replay from offset — like Kafka consumer reset
    public void resetOffset(String consumerGroup, int toOffset) {
        consumerOffsets.computeIfAbsent(consumerGroup, k -> new AtomicInteger(0))
                .set(toOffset);
        System.out.printf("[Kafka:%s] Consumer group '%s' reset to offset %d%n",
                topicName, consumerGroup, toOffset);
    }

    public int size() { return log.size(); }
    public String getTopicName() { return topicName; }
}