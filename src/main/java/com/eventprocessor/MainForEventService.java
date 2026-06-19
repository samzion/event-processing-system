package com.eventprocessor;

import com.eventprocessor.metrics.EventLog;
import com.eventprocessor.metrics.EventMetrics;
import com.eventprocessor.metrics.EventTypeCounter;
import com.eventprocessor.model.Event;
import com.eventprocessor.model.ProcessingResult;
import com.eventprocessor.processor.EventProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MainForEventService {

    private static final int THREAD_POOL_SIZE = 4;

    public static void main(String[] args) throws InterruptedException {

        Thread.setDefaultUncaughtExceptionHandler((t, e) ->
                System.err.printf("[UNCAUGHT] Thread '%s' threw: %s%n", t.getName(), e.getMessage())
        );

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

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        long startTime = System.currentTimeMillis();

        // Build a CompletableFuture pipeline per event
        List<CompletableFuture<ProcessingResult>> futures = incomingEvents.stream()
                .map(event -> {
                    int index = incomingEvents.indexOf(event) + 1;
                    EventProcessor processor = new EventProcessor(
                            index, event, metrics, eventLog, eventTypeCounter
                    );

                    return CompletableFuture
                            // Run the processor asynchronously on our pool
                            .supplyAsync(() -> {
                                try {
                                    return processor.call();
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            }, pool)
                            // When done, log the result — triggers automatically, no blocking
                            .thenApply(result -> {
                                System.out.printf("Pipeline: %s completed in %dms%n",
                                        result.eventId(), result.durationMs());
                                return result;
                            })
                            // If this task failed, recover with a failed result instead of crashing
                            .exceptionally(e -> {
                                System.err.printf("Pipeline: event %s failed — %s%n",
                                        event.getId(), e.getCause().getMessage());
                                return new ProcessingResult(event.getId(), index, 0, false);
                            });
                })
                .collect(Collectors.toList());

        // Wait for all pipelines to complete — non-blocking until this point
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        long elapsed = System.currentTimeMillis() - startTime;

        pool.shutdown();
        boolean cleanExit = pool.awaitTermination(10, TimeUnit.SECONDS);
        if (!cleanExit) {
            System.err.println("Pool did not terminate cleanly — forcing shutdown");
            pool.shutdownNow();
        }

        metrics.printSummary();

        System.out.printf("All events processed in %dms%n", elapsed);
        eventLog.print();
        eventTypeCounter.printCounts();
    }
}
