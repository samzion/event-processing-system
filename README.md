# Real-Time Event Processing System

A production-grade Java concurrency and distributed systems project built progressively across 12 phases — from raw OS threads to distributed transaction patterns. Each phase introduces a specific concurrency primitive or architectural pattern, anchored to real fintech scenarios applicable to the Nigerian payments ecosystem.

> **Stack:** Java 21 · Pure Java (no frameworks) · IntelliJ IDEA  
> **Branches:** `main` (latest) · `phase-5` through `phase-12` (one branch per phase)

---

## CBN Data Residency Policy — January 2027

The Central Bank of Nigeria issued a circular on June 15, 2026 directing all banks, fintechs, and payment service providers to ensure that all payment transaction data generated within Nigeria is stored and managed domestically by January 1, 2027. The directive ends Nigeria's dependence on foreign-hosted infrastructure for sensitive payment data — a shift that affects nearly 90% of regulated businesses currently relying on AWS, Azure, and Google Cloud.

The policy creates a specific engineering challenge: migrating transaction processing, fraud detection pipelines, and event streaming from global cloud infrastructure to locally hosted data centres — while maintaining the resilience and throughput that currently comes from those global providers.

The architectural patterns demonstrated in this project directly address what that migration requires:

- **Backpressure (Phase 9):** Local infrastructure has less headroom than AWS. Bounded queues with `CallerRunsPolicy` prevent crashes under salary-day transaction spikes on constrained local infrastructure.
- **Persistent event log with replay (Phase 11):** Kafka-style offset tracking and replay enables regulatory audit trails and disaster recovery without foreign backup dependencies — addressing the CBN's identified gap in local disaster recovery capability.
- **Idempotency (Phase 12):** When local data centres fail over and transactions are retried, `putIfAbsent`-based idempotency keys guarantee exactly-once processing. Money never moves twice regardless of infrastructure failure.
- **Async validation pipelines (Phase 6):** Concurrent fraud, balance, and KYC checks reduce authorisation latency — making domestic processing competitive with offshore in terms of user experience.
- **Virtual threads (Phase 7):** Handling thousands of concurrent USSD sessions and payment requests on locally hosted Spring Boot services without the memory cost of large platform thread pools.

Nigeria currently spends approximately $850 million annually on offshore data hosting. This project demonstrates the backend engineering architecture needed to bring that processing home reliably.

---

## Why This Project Exists

Most concurrency tutorials teach concepts in isolation. This project builds a single evolving system — a real-time financial event processing pipeline — and upgrades it phase by phase. Each upgrade solves a concrete problem introduced by the previous phase. The result is a codebase that demonstrates not just what each tool does, but *why* it exists and what breaks without it.

Every phase is anchored to a real Nigerian fintech scenario. The problems are not academic.

---

## System Overview

The system simulates a fintech event pipeline handling three event types:

| Event Type | Processing Time | Fintech Analogy |
|---|---|---|
| `PRICE_UPDATE` | 50ms | Exchange rate tick |
| `ALERT` | 100ms | Fraud signal |
| `TRADE_CONFIRMATION` | 200ms | Settlement instruction |

Each phase evolves how these events are produced, processed, tracked, and coordinated.

---

## Phase Breakdown

### Phase 1 — Raw Java Threads
**Fintech context:** Every USSD request spawns a thread. At peak load on salary day, millions of Nigerians hitting `*242#` simultaneously. Raw threads collapse.

One thread per event. 10 events → 10 threads created and destroyed.

- Implements `Runnable` (not `extends Thread`) — decouples task logic from execution mechanism
- Named threads for readable output and production thread dumps
- `UncaughtExceptionHandler` to prevent silent thread failures
- `thread.join()` for coordinated shutdown

**Problem introduced:** At 10,000 events/second — 5GB RAM in stacks alone, 1 second wasted on thread creation, OS scheduler collapses under context switch pressure.

---

### Phase 2 — ExecutorService
**Fintech context:** Paga's payment gateway — fixed thread pool sized to DB connection limit.

Fixed thread pool of 4 workers. Tasks queue when all threads are busy. Threads reused across tasks.

- `Callable<ProcessingResult>` replacing `Runnable` — structured return values
- `Future<ProcessingResult>` for result collection and per-task failure handling
- `ExecutionException` unwrapping — transfers exceptions from worker threads to caller
- Orderly shutdown with `awaitTermination`

**Key observation:** 10 tasks processed by 4 threads — same thread names appear multiple times in output, confirming reuse. No thread creation overhead per task.

---

### Phase 3 — `synchronized`
**Fintech context:** Paga wallet balance — two concurrent debits without synchronization cause lost updates. Both threads read ₦5,000, both deduct ₦3,000, both write ₦2,000. Customer loses ₦3,000. Paga loses ₦3,000.

Shared `EventMetrics` object updated concurrently by all worker threads.

- Demonstrated race condition: 10,000 tasks without synchronization → 9,887 counted (113 lost updates, zero errors thrown)
- With `synchronized`: exact count every run
- Measured lock contention cost: 10,000 tasks took **295 seconds** with `synchronized` vs **1.3 seconds** without
- `EventLog` with `synchronized ArrayList` — thread-safe append across concurrent completions

**Key lesson:** Synchronization gives correctness but serialises concurrent work at the lock point. Contention at scale is catastrophic.

---

### Phase 4 — Atomic Classes
**Fintech context:** Transaction counters, daily limit trackers, fee accumulators — single-variable updates that need correctness without lock overhead.

Replaced `synchronized` counters in `EventMetrics` with `AtomicInteger` and `AtomicLong`.

- CAS (Compare-And-Swap) — single CPU instruction (`CMPXCHG`), no thread blocking
- Implemented CAS loop pattern for conditional atomic update (`if counter < limit, increment`)
- Measured: atomics vs `synchronized` under extreme contention — `synchronized` faster at zero-sleep due to CAS spin overhead; atomics faster under realistic I/O-bound load

**Key lesson:** Atomics win under low-to-medium contention. `LongAdder` preferred for extreme contention counters.

---

### Phase 5 — ConcurrentHashMap
**Fintech context:** Real-time NGN/USD exchange rate cache — multiple threads reading rates while a background thread updates them.

Added `EventTypeCounter` — concurrent per-type event counting and last-payload tracking.

- `merge(key, 1, Integer::sum)` — atomic per-key increment without external locking
- `put()` for last-payload tracking — last-write-wins acceptable for observability fields
- Demonstrated why `get()` then `put()` is a race condition even on `ConcurrentHashMap`
- Bucket-level locking: different keys → zero contention between threads

---

### Phase 6 — CompletableFuture
**Fintech context:** Processing a Paga transfer — validate sender, validate receiver, check fraud score, check daily limits — all four run concurrently, results combined before debiting.

Replaced blocking `Future.get()` collection loop with a declarative async pipeline per event.

- `supplyAsync(supplier, executor)` — always explicit executor, never default `ForkJoinPool`
- `thenApply` — transform result on completion, no blocking thread
- `exceptionally` — per-event failure recovery without crashing the pipeline
- `CompletableFuture.allOf().join()` — wait for all pipelines without sequential blocking

**Key shift:** From pull-based (`Future.get()` blocks) to push-based (pipeline triggers automatically on completion).

---

### Phase 7 — Virtual Threads (Java 21, JEP 444)
**Fintech context:** Paga's Spring Boot API handling thousands of concurrent USSD sessions without a massive platform thread pool.

Replaced `Executors.newFixedThreadPool(4)` with `Executors.newVirtualThreadPerTaskExecutor()`.

- Single line change — all 10 events start simultaneously with no queuing
- Virtual threads: ~1KB vs 512KB for platform threads. Unmount on I/O, remount on completion.
- Discussed pinning: `synchronized` + blocking I/O pins carrier thread; `ReentrantLock` does not
- Production application: Spring Boot 3.5 + Java 21 + `spring.threads.virtual.enabled=true`
- CBN relevance: locally hosted Spring Boot services handling high USSD concurrency without the memory cost that would require oversized local servers

---

### Phase 8 — Producer-Consumer Pattern
**Fintech context:** Paga's settlement engine — API layer produces transfer requests at unpredictable rates, settlement processes at its own pace. Salary-day spike absorbed by the queue.

Decoupled event ingestion from processing using `BlockingQueue`.

- `ArrayBlockingQueue(5)` — bounded queue, backpressure applied when full
- Producer sleeps 20ms between events; 3 consumers process at 50–200ms
- Poison pill pattern for clean multi-consumer shutdown — re-enqueued for cascade
- `AtomicInteger` shared processor ID counter across consumers

**Key observation:** Producer visibly blocks when queue fills. All 3 consumers receive the poison pill and shut down cleanly in sequence.

---

### Phase 9 — ThreadPoolExecutor Deep Dive
**Fintech context:** Paga's settlement pool — explicitly sized to 20 matching the DB connection pool limit. `CallerRunsPolicy` slows ingestion when settlement falls behind.

Replaced factory method with explicit `ThreadPoolExecutor` construction.

- Named thread factory: `event-worker-{id}` — readable in production thread dumps
- `ArrayBlockingQueue(10)` — bounded work queue with explicit backpressure
- `CallerRunsPolicy` — producer thread runs task when pool is at capacity (natural backpressure, no dropped events)
- Exposed pool metrics: `getCompletedTaskCount()`, `getLargestPoolSize()`, `getTaskCount()`
- Debugged and fixed real deadlock: consumers filled core threads before producer got a slot — resolved by setting `corePoolSize = CONSUMER_COUNT + 1`

---

### Phase 10 — Event-Driven Architecture
**Fintech context:** Paga transfer completion — SMS receipt, ledger bookkeeping, fraud model update, and analytics dashboard all triggered by one event. Transfer Service has zero knowledge of any downstream consumer.

In-process event bus decoupling emitter from subscribers.

- `ConcurrentHashMap<String, List<Consumer<DomainEvent>>>` — per-event-type handler registry
- `CopyOnWriteArrayList` for handler list — safe concurrent reads during publish
- Fire-and-forget publish: each handler runs on pool thread, emitter returns immediately
- Adding a new downstream consumer requires zero changes to the emitter

**Demonstrated:** Receipt Service, Ledger Service, and Fraud Service all handled `TRANSFER_COMPLETED` concurrently and independently. Non-deterministic completion order — correct by design.

---

### Phase 11 — Kafka Simulation
**Fintech context:** Paga's audit trail — every transaction published to a persistent log. Compliance service, fraud detection, and analytics all consume independently. Regulators can demand replay of any historical period.

Simulated Apache Kafka topic with persistent log, offset tracking, and replay.

- Append-only log — events survive consumption, unlike a traditional queue
- Independent consumer group offsets: `fraud-detection` and `analytics` read the same topic independently at their own pace
- Offset reset and replay — fraud service reprocesses all events from offset 0 on demand
- CBN relevance: regulatory audit trail replay is a compliance requirement. Kafka's retention model makes this native. A traditional queue where consumed messages are deleted cannot satisfy this requirement.

**Discussed:** Spring Kafka `@KafkaListener`, partition key for ordering guarantees, Kafka vs RabbitMQ tradeoffs, migration from AWS-hosted Kafka to locally hosted clusters for CBN compliance.

---

### Phase 12 — Distributed Systems Foundations
**Fintech context:** Paga's wallet service and settlement service on separate servers. Network dies after debit, before credit. Money lost. The distributed transaction problem.

Idempotent transfer service solving the duplicate-processing problem under concurrent retries.

- `putIfAbsent` as atomic claim — only one thread processes a given idempotency key
- PROCESSING sentinel pattern — concurrent retries poll until real result replaces sentinel
- Demonstrated: 3 concurrent requests with the same key → 1 processes, 2 return cached result
- Covered: CAP theorem (CP for balances, AP for dashboards), Saga pattern vs 2PC, eventual consistency

**Real-world application:** Nigerian mobile networks are unreliable. A customer submits a transfer and the connection drops. Without idempotency, retrying the request sends money twice. With idempotency keys, money moves exactly once regardless of how many times the client retries.

**CBN relevance:** When local data centres fail over — a realistic risk given the concentration of Nigerian data centres in Lagos and the absence of geographically distributed availability zones — transactions will be retried. Idempotency is the guarantee that makes failover safe.

---

## Key Measurements

| Scenario | Result |
|---|---|
| 10 tasks, raw threads | ~400ms, 10 threads created and destroyed |
| 10 tasks, fixed pool (4 threads) | ~400ms, 4 threads reused |
| 10,000 tasks, `synchronized` counter | **295 seconds** — lock contention |
| 10,000 tasks, atomic counter | **1.3 seconds** — CAS, no blocking |
| 10 tasks, fixed pool (4 threads) | ~410ms — tasks processed in waves |
| 10 tasks, virtual threads | ~220ms — all start simultaneously |

---

## Project Structure

```
src/com/eventprocessor/
├── model/
│   ├── Event.java                      # Domain model — PRICE_UPDATE, ALERT, TRADE_CONFIRMATION
│   └── ProcessingResult.java           # Record — eventId, processorId, durationMs, success
├── processor/
│   └── EventProcessor.java             # Callable<ProcessingResult> — core task unit
├── producer/
│   └── EventProducer.java              # Runnable — feeds BlockingQueue
├── consumer/
│   └── EventConsumer.java              # Runnable — drains BlockingQueue
├── metrics/
│   ├── EventMetrics.java               # AtomicInteger/AtomicLong counters
│   ├── EventLog.java                   # synchronized ArrayList
│   └── EventTypeCounter.java           # ConcurrentHashMap merge()
├── eventbus/
│   ├── DomainEvent.java                # Record — eventType, payload, timestampMs
│   └── EventBus.java                   # In-process pub/sub with ConcurrentHashMap
├── kafka/
│   └── SimulatedKafkaTopic.java        # Persistent log, offset tracking, replay
├── distributed/
│   └── IdempotentTransferService.java  # putIfAbsent sentinel pattern
└── Main*.java                          # Entry point per phase
```

---

## Concepts Covered

**Threading:** OS thread model, thread lifecycle, context switching, stack memory costs, `start()` vs `run()`, `InterruptedException` contract

**Synchronization:** Race conditions, lost updates, Java Memory Model, `synchronized` atomicity and visibility, deadlock

**Lock-Free Concurrency:** CAS, `AtomicInteger`, `AtomicLong`, `LongAdder`, CAS loop pattern

**Concurrent Collections:** `ConcurrentHashMap`, `ArrayBlockingQueue`, `CopyOnWriteArrayList`, compound atomic operations

**Async Programming:** `CompletableFuture`, `thenApply`, `thenCompose`, `allOf`, `exceptionally`, `handle`

**Modern Java:** Virtual threads (JEP 444), carrier thread pinning, `ReentrantLock` vs `synchronized`

**Architecture:** Producer-Consumer, Event-Driven Architecture, in-process event bus

**Distributed Systems:** CAP theorem, Saga pattern, 2PC, idempotency, eventual consistency, Kafka offset model

---

## Related Projects

- **[TourneyOps](https://tourneyops.com)** — Live SaaS tournament management platform (Java 17, Spring Boot 3.5, PostgreSQL, AWS) where these patterns are applied in production
- **[RoutePilot](https://github.com/samzion/route-pilot-be)** — Lagos logistics API with delivery state machine
- **[PowerPulse](https://github.com/samzion/powerpulse)** — Nigerian energy monitoring system for SMEs
