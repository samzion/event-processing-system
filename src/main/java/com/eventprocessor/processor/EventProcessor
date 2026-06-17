package main.java.com.eventprocessor.processor;

import main.java.com.eventprocessor.metrics.EventLog;
import main.java.com.eventprocessor.metrics.EventMetrics;
import main.java.com.eventprocessor.model.Event;
import main.java.com.eventprocessor.model.ProcessingResult;

import java.util.concurrent.Callable;

public class EventProcessor implements Callable<ProcessingResult> {

    private final Event event;
    private final int processorId;
    private final EventMetrics metrics;
    private final EventLog eventLog;

    public EventProcessor(int processorId, Event event, EventMetrics metrics, EventLog eventLog) {
        this.processorId = processorId;
        this.event = event;
        this.metrics = metrics;
        this.eventLog = eventLog;
    }

    @Override
    public ProcessingResult call() {
        String threadName = Thread.currentThread().getName();

        System.out.printf("[%s] Processor-%d starting: %s%n",
                threadName, processorId, event);

        long startTimeMs = System.currentTimeMillis();

        if (processorId == 3) {
            metrics.recordFailure();
            throw new RuntimeException("Simulated failure in processor " + processorId);
        }

        try {
            long processingTimeMs = switch (event.getType()) {
                case PRICE_UPDATE -> 50;
                case TRADE_CONFIRMATION -> 200;
                case ALERT -> 100;
            };

            Thread.sleep(processingTimeMs);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.printf("[%s] Processor-%d was interrupted for event: %s%n",
                    threadName, processorId, event.getId());
            metrics.recordFailure();
            return new ProcessingResult(event.getId(), processorId,
                    System.currentTimeMillis() - startTimeMs, false);
        }

        long duration = System.currentTimeMillis() - startTimeMs;
        metrics.recordSuccess(duration);

        String logEntry = String.format("ProcessingResult[eventId=%s, processorId=%d, durationMs=%d, success=true]",
                event.getId(), processorId, duration);
        eventLog.append(logEntry);

        System.out.printf("[%s] Processor-%d completed: %s%n",
                threadName, processorId, event);

        return new ProcessingResult(event.getId(), processorId, duration, true);
    }
}
