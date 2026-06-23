package com.eventprocessor.distributed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IdempotentTransferService {

    // Stores processed idempotency keys and their results
    private final Map<String, TransferResult> processedRequests = new ConcurrentHashMap<>();

    public record TransferRequest(
            String idempotencyKey,
            String sender,
            String receiver,
            long amountKobo
    ) {}

    public record TransferResult(
            String idempotencyKey,
            boolean success,
            String message,
            long processedAtMs
    ) {}

    public TransferResult transfer(TransferRequest request) {

        // Return cached result if already processed — idempotent
        TransferResult existing = processedRequests.get(request.idempotencyKey());
        if (existing != null && !existing.message().equals("PROCESSING")) {
            System.out.printf("[TransferService] Duplicate request detected for key: %s — " +
                    "returning cached result%n", request.idempotencyKey());
            return existing;
        }

        // Use a sentinel to claim this key before processing
        TransferResult sentinel = new TransferResult(
                request.idempotencyKey(), false, "PROCESSING", System.currentTimeMillis()
        );

        // Only one thread wins this — others get the sentinel back
        TransferResult claimed = processedRequests.putIfAbsent(
                request.idempotencyKey(), sentinel
        );

        if (claimed != null) {
            // Another thread already claimed this key
            System.out.printf("[TransferService] Duplicate detected for key: %s%n",
                    request.idempotencyKey());
            // Wait briefly for the real result to be written
            int attempts = 0;
            while (attempts < 20) {
                TransferResult current = processedRequests.get(request.idempotencyKey());
                if (current != null && !current.message().equals("PROCESSING")) {
                    return current;
                }
                try { Thread.sleep(50); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                attempts++;
            }
            return processedRequests.get(request.idempotencyKey());
        }

        // Process the transfer
        System.out.printf("[TransferService] Processing: %s → %s ₦%.2f (key: %s)%n",
                request.sender(), request.receiver(),
                request.amountKobo() / 100.0,
                request.idempotencyKey());

        // Simulate processing time
        try { Thread.sleep(100); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        TransferResult result = new TransferResult(
                request.idempotencyKey(),
                true,
                String.format("Transfer of ₦%.2f from %s to %s completed",
                        request.amountKobo() / 100.0, request.sender(), request.receiver()),
                System.currentTimeMillis()
        );

        // Replace sentinel with real result
        processedRequests.put(request.idempotencyKey(), result);
        System.out.printf("[TransferService] Completed: %s%n", result.message());
        return result;
    }
}