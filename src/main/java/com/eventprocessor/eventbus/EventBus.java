package com.eventprocessor.eventbus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class EventBus {

    // One handler list per event type
    private final Map<String, List<Consumer<DomainEvent>>> handlers =
            new ConcurrentHashMap<>();

    private final ExecutorService executor;

    public EventBus(ExecutorService executor) {
        this.executor = executor;
    }

    // Register a handler for an event type
    public void subscribe(String eventType, Consumer<DomainEvent> handler) {
        handlers.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>())
                .add(handler);
    }

    // Publish — fire and forget, handlers run async
    public void publish(DomainEvent event) {
        List<Consumer<DomainEvent>> subscribers =
                handlers.getOrDefault(event.eventType(), List.of());

        for (Consumer<DomainEvent> handler : subscribers) {
            executor.submit(() -> {
                try {
                    handler.accept(event);
                } catch (Exception e) {
                    System.err.printf("[EventBus] Handler failed for %s: %s%n",
                            event.eventType(), e.getMessage());
                }
            });
        }
    }

    public void shutdown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}