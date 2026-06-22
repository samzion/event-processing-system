package com.eventprocessor;

import com.eventprocessor.eventbus.DomainEvent;
import com.eventprocessor.kafka.SimulatedKafkaTopic;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MainKafkaSimulation {

    public static void main(String[] args) throws InterruptedException {

        SimulatedKafkaTopic topic = new SimulatedKafkaTopic("payment-events");

        var executor = Executors.newFixedThreadPool(4);

        // Producer — simulates Paga's Transfer Service
        executor.submit(() -> {
            try {
                topic.publish(new DomainEvent("TRANSFER_COMPLETED",
                        "TXN001: Samson → Emmanuel ₦5000"));
                Thread.sleep(50);
                topic.publish(new DomainEvent("TRANSFER_COMPLETED",
                        "TXN002: Emmanuel → Paga ₦12000"));
                Thread.sleep(50);
                topic.publish(new DomainEvent("TRANSFER_COMPLETED",
                        "TXN003: Paga → Samson ₦8000"));
                Thread.sleep(50);
                topic.publish(new DomainEvent("TRANSFER_COMPLETED",
                        "TXN004: Samson → Paga ₦3000"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(300); // let producer finish

        // Consumer Group 1: Fraud Detection Service
        executor.submit(() -> {
            System.out.println("\n[FraudService] Starting consumption...");
            List<DomainEvent> events = topic.poll("fraud-detection", 10);
            for (DomainEvent event : events) {
                System.out.printf("[FraudService] Analysing: %s%n", event.payload());
            }
        });

        // Consumer Group 2: Analytics Service — independent offset
        executor.submit(() -> {
            System.out.println("\n[AnalyticsService] Starting consumption...");
            List<DomainEvent> events = topic.poll("analytics", 10);
            for (DomainEvent event : events) {
                System.out.printf("[AnalyticsService] Recording: %s%n", event.payload());
            }
        });

        Thread.sleep(500);

        // Demonstrate replay — fraud service reprocesses from offset 0
        System.out.println("\n--- Replaying from offset 0 for audit ---");
        topic.resetOffset("fraud-detection", 0);
        List<DomainEvent> replayed = topic.poll("fraud-detection", 10);
        for (DomainEvent event : replayed) {
            System.out.printf("[FraudService] Replaying: %s%n", event.payload());
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("\nSimulation complete.");
    }
}