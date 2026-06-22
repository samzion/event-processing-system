package com.eventprocessor.producer;

import com.eventprocessor.model.Event;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class EventProducer implements Runnable {

    private final BlockingQueue<Event> queue;
    private final List<Event> events;
    private final Event poisonPill;

    public EventProducer(BlockingQueue<Event> queue, List<Event> events, Event poisonPill) {
        this.queue = queue;
        this.events = events;
        this.poisonPill = poisonPill;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        try {
            for (Event event : events) {
                System.out.printf("[%s] Producing: %s%n", threadName, event.getId());
                queue.put(event);
                // Simulate events arriving over time
                Thread.sleep(20);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.printf("[%s] Producer interrupted%n", threadName);
        } finally {
            // Always send poison pill — even if interrupted
            try {
                queue.put(poisonPill);
                System.out.printf("[%s] Producer sent poison pill%n", threadName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}