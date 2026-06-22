package com.eventprocessor;

import com.eventprocessor.consumer.EventConsumer;
import com.eventprocessor.metrics.EventLog;
import com.eventprocessor.metrics.EventMetrics;
import com.eventprocessor.metrics.EventTypeCounter;
import com.eventprocessor.model.Event;
import com.eventprocessor.producer.EventProducer;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MainProducerConsumer {

    private static final int CONSUMER_COUNT = 3;
    private static final int QUEUE_CAPACITY = 5; // deliberately small to show backpressure

    public static void main(String[] args) throws InterruptedException {

        // Poison pill — identity matters, not content
        Event poisonPill = new Event("POISON", null, null);

        BlockingQueue<Event> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        List<Event> incomingEvents = List.of(
                new Event("E001", Event.EventType.PRICE_UPDATE,       "BTC/USD: 65000"),
                new Event("E002", Event.EventType.TRADE_CONFIRMATION, "Buy 10 BTC @ 65000"),
                new Event("E003", Event.EventType.ALERT,              "Volatility spike detected"),
                new Event("E004", Event.EventType.PRICE_UPDATE,       "ETH/USD: 3200"),
                new Event("E005", Event.EventType.TRADE_CONFIRMATION, "Sell 5 ETH @ 3200"),
                new Event("E006", Event.EventType.PRICE_UPDATE,       "SOL/USD: 145"),
                new Event("E007", Event.EventType.ALERT,              "Circuit breaker triggered"),
                new Event("E008", Event.EventType.TRADE_CONFIRMATION, "Buy 100 SOL @ 145"),
                new Event("E009", Event.EventType.PRICE_UPDATE,       "ADA/USD: 0.45"),
                new Event("E010", Event.EventType.ALERT,              "Daily limit approaching")
        );

        EventMetrics metrics = new EventMetrics();
        EventLog eventLog = new EventLog();
        EventTypeCounter eventTypeCounter = new EventTypeCounter();
        AtomicInteger processorIdCounter = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(CONSUMER_COUNT + 1);

        long startTime = System.currentTimeMillis();

        // Start consumers first — they block on empty queue until producer feeds them
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            executor.submit(new EventConsumer(queue, metrics, eventLog,
                    eventTypeCounter, poisonPill, processorIdCounter));
        }

        // Start producer
        executor.submit(new EventProducer(queue, incomingEvents, poisonPill));

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        long elapsed = System.currentTimeMillis() - startTime;

        metrics.printSummary();
        System.out.printf("All events processed in %dms%n", elapsed);
        eventTypeCounter.printCounts();
    }
}