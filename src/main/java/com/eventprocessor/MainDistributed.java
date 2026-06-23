package com.eventprocessor;

import com.eventprocessor.distributed.IdempotentTransferService;
import com.eventprocessor.distributed.IdempotentTransferService.TransferRequest;
import com.eventprocessor.distributed.IdempotentTransferService.TransferResult;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MainDistributed {

    public static void main(String[] args) throws InterruptedException {

        IdempotentTransferService service = new IdempotentTransferService();
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Normal transfer
        TransferRequest request1 = new TransferRequest(
                "TXN-UUID-001", "Samson", "Emmanuel", 500000L // ₦5000.00
        );

        // Simulate network retry — same idempotency key sent 3 times
        // This happens when client times out and retries
        System.out.println("=== Simulating network retry scenario ===\n");
        for (int i = 1; i <= 3; i++) {
            int attempt = i;
            executor.submit(() -> {
                System.out.printf("--- Attempt %d ---%n", attempt);
                TransferResult result = service.transfer(request1);
                System.out.printf("Result: success=%s, msg=%s%n\n",
                        result.success(), result.message());
            });
            Thread.sleep(50); // slight delay between retries
        }

        Thread.sleep(500);

        // Different idempotency key — genuinely new transfer
        System.out.println("=== New transfer with different key ===\n");
        TransferRequest request2 = new TransferRequest(
                "TXN-UUID-002", "Emmanuel", "Paga", 1200000L // ₦12000.00
        );
        TransferResult result2 = service.transfer(request2);
        System.out.printf("Result: %s%n", result2.message());

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("\nDistributed simulation complete.");
    }
}