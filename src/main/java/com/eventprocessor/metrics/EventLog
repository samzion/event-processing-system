package main.java.com.eventprocessor.metrics;

import java.util.ArrayList;
import java.util.List;

public class EventLog {

    private final List<String> log = new ArrayList<>();

    public synchronized void append(String entry) {
        log.add(entry);
    }

    public synchronized void print() {
        log.forEach(System.out::println);
    }
}
