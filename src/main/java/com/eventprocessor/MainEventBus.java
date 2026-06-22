package com.eventprocessor;

import com.eventprocessor.eventbus.DomainEvent;
import com.eventprocessor.eventbus.EventBus;

import java.util.concurrent.Executors;

public class MainEventBus {

    public static void main(String[] args) throws InterruptedException {

        EventBus bus = new EventBus(Executors.newFixedThreadPool(4));

        // Receipt service — sends confirmation SMS
        bus.subscribe("TRANSFER_COMPLETED", event -> {
            System.out.printf("[ReceiptService] Sending SMS for: %s%n", event.payload());
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.printf("[ReceiptService] SMS sent for: %s%n", event.payload());
        });

        // Ledger service — records double-entry bookkeeping
        bus.subscribe("TRANSFER_COMPLETED", event -> {
            System.out.printf("[LedgerService] Recording ledger entry: %s%n", event.payload());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.printf("[LedgerService] Ledger updated: %s%n", event.payload());
        });

        // Fraud service — feeds detection model
        bus.subscribe("TRANSFER_COMPLETED", event -> {
            System.out.printf("[FraudService] Analysing: %s%n", event.payload());
            try {
                Thread.sleep(80);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.printf("[FraudService] Analysis complete: %s%n", event.payload());
        });

        // Publish three transfer events
        bus.publish(new DomainEvent("TRANSFER_COMPLETED", "TXN001: Samson → Emmanuel ₦5000"));
        bus.publish(new DomainEvent("TRANSFER_COMPLETED", "TXN002: Emmanuel → Paga ₦12000"));
        bus.publish(new DomainEvent("TRANSFER_COMPLETED", "TXN003: Paga → Samson ₦8000"));

        Thread.sleep(1000); // let handlers complete
        bus.shutdown();

        System.out.println("\nAll events processed.");
    }
}