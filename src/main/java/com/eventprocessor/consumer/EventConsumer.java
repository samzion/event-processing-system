package com.eventprocessor.consumer;

import com.eventprocessor.metrics.EventLog;
import com.eventprocessor.metrics.EventMetrics;
import com.eventprocessor.metrics.EventTypeCounter;
import com.eventprocessor.model.Event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class EventConsumer implements Runnable {

    private final BlockingQueue<Event> queue;
    private final EventMetrics metrics;
    private final EventLog eventLog;
    private final EventTypeCounter eventTypeCounter;
    private final Event poisonPill;
    private final AtomicInteger processorIdCounter;

    public EventConsumer(BlockingQueue<Event> queue, EventMetrics metrics,
                         EventLog eventLog, EventTypeCounter eventTypeCounter,
                         Event poisonPill, AtomicInteger processorIdCounter) {
        this.queue = queue;
        this.metrics = metrics;
        this.eventLog = eventLog;
        this.eventTypeCounter = eventTypeCounter;
        this.poisonPill = poisonPill;
        this.processorIdCounter = processorIdCounter;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();

        while (true) {
            try {
                Event event = queue.take(); // blocks until event available

                // Poison pill check — reference equality intentional
                if (event == poisonPill) {
                    System.out.printf("[%s] Consumer received poison pill — shutting down%n",
                            threadName);
                    queue.put(poisonPill); // pass it on for other consumers
                    break;
                }

                int processorId = processorIdCounter.incrementAndGet();
                long startTime = System.currentTimeMillis();

                System.out.printf("[%s] Consuming: %s%n", threadName, event.getId());

                long processingTimeMs = switch (event.getType()) {
                    case PRICE_UPDATE -> 50;
                    case TRADE_CONFIRMATION -> 200;
                    case ALERT -> 100;
                };

                Thread.sleep(processingTimeMs);

                long duration = System.currentTimeMillis() - startTime;
                metrics.recordSuccess(duration);
                eventTypeCounter.record(event.getType().name(), event.getPayload());
                eventLog.append(String.format("Consumed: %s in %dms by %s",
                        event.getId(), duration, threadName));

                System.out.printf("[%s] Completed: %s in %dms%n",
                        threadName, event.getId(), duration);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.printf("[%s] Consumer interrupted%n", threadName);
                break;
            }
        }
    }
}