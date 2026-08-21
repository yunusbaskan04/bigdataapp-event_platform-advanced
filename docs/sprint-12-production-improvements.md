# Sprint 12 - Production Improvements & Hardening in Kafka

## Goal

Harden our Event-Driven Platform for production readiness by implementing **Producer Idempotency (Exactly-Once Producer Semantics)**, **Schema Management concepts**, and **Classified Consumer Error Handling with Dead Letter Topics (DLT)**.

Ensure zero data loss, zero message duplication, and resilient error recovery under real-world production network glitches, poison pills, and schema evolutions.

---

## Theoretical Concepts & Architecture

### 1. Schema Registry & Binary Serialization (Apache Avro)

In development, **JSON** is popular due to human readability. However, in production big data systems, JSON introduces major risks:
- **Silent Schema Breakage:** If a Producer renames `"orderId"` to `"order_id"` or deletes a required field, Consumers fail silently or crash at runtime.
- **High Payload Overhead:** JSON repeats field key strings (`{"orderId":101, "product":"Laptop"}`) in every single message.

#### Production Solution: Apache Avro & Schema Registry
- **Binary Format:** Transmits compact binary data without key strings. Payload size drops by **60-80%**, saving massive network bandwidth and disk storage.
- **Schema Registry:** Acts as a central schema repository. Before publishing, the Producer validates the schema against the Registry. If a schema change is incompatible (**Backward Compatibility Violation**), the Registry rejects the publish *at compile/publish time*, preventing broken messages from reaching Kafka.

---

### 2. Idempotent Producer & Exactly-Once Producer Semantics

In standard Kafka setups, network glitches cause duplicate records:
$$\text{Producer} \xrightarrow{\text{Send Event}} \text{Broker Writes (Offset 50)} \xrightarrow{\text{ACK Lost in Network}} \text{Producer Retries} \xrightarrow{\text{Broker Writes Duplicate (Offset 51)}}$$

#### Solution: `enable.idempotence=true`
When `enable.idempotence=true` is set:
1. **Producer ID (PID):** The Kafka Broker automatically assigns a unique PID to the Producer client on startup.
2. **Sequence Number:** The Producer automatically attaches a monotonically increasing sequence number (0, 1, 2...) to every record per partition.
3. **Automatic Deduplication:** If the Broker receives a record with a sequence number it has already written, it drops the duplicate on disk while returning an ACK to the Producer.

> 💡 **Developer Action Required?** **NO!** Setting `enable.idempotence=true` handles PID assignment, sequence numbering, and deduplication **100% automatically** inside the Kafka client and broker.

#### Key Producer Properties Breakdown

| Property | Value | Purpose |
| :--- | :--- | :--- |
| `enable.idempotence` | `true` | Enables PID & Sequence Number deduplication automatically. |
| `acks` | `all` (or `-1`) | Forces all In-Sync Replicas (ISR) to acknowledge record write. |
| `retries` | `2147483647` (`Integer.MAX_VALUE`) | Instructs producer to retry transient failures continuously. |
| `delivery.timeout.ms` | `120000` (2 minutes) | The hard upper bound time limit for retries before failing. |
| `max.in.flight.requests.per.connection` | `5` (or `1`) | Guarantees record ordering across retries. |

> ❓ **Why `retries = Integer.MAX_VALUE`?**
> Setting `retries` to MAX does NOT cause an infinite loop. Modern Kafka uses `delivery.timeout.ms` (default 2 minutes) as the maximum time ceiling. The producer will retry as many times as possible *within that 2-minute window* during a temporary broker outage. Combined with idempotence, this guarantees **Zero Data Loss + Zero Duplicate Records**.

---

### 3. Idempotent Consumer Strategies

While `enable.idempotence=true` prevents duplicates *on the Kafka cluster*, duplicates can still reach a Consumer if the Consumer crashes before committing offsets.

#### Production Solution: Database Constraints
1. **Primary Key / Unique Index:** Set `order_id` as the `PRIMARY KEY` or `UNIQUE INDEX` in the Consumer's database.
2. **`ON CONFLICT DO NOTHING` / Exception Handling:** If a duplicate event arrives, the database rejects duplicate insertion (`UniqueConstraintViolationException`), ensuring idempotent processing.
3. **Processed Events Table (Deduplication Log):** Store processed `event_id`s in a dedicated table within the same database transaction.

---

### 4. Classified Error Handling & Poison Pills

Not all exceptions are created equal:
1. **Retryable Exceptions (Geçici Hatalar):** Transient DB timeouts, network glitches.
   - *Strategy:* Use **Exponential Backoff** (e.g., wait 1s, 2s, 4s...) and retry 3 times.
2. **Non-Retryable Exceptions (Poison Pills / Kalıcı Hatalar):** Malformed JSON, missing fields, `DeserializationException`.
   - *Strategy:* **0 Retries!** Immediately route to **Dead Letter Topic (DLT)** to prevent Head-of-Line blocking.

```text
Incoming Message
       │
       ▼
 [Order Consumer]
       │
       ├──► (Success) ──► Commit Offset
       │
       ├──► (Retryable Exception: DB Timeout) ──► Exponential Backoff (1s, 2s, 4s) ──► Retry (Max 3)
       │                                                                                   │
       │                                                                                   ▼ (If still fails)
       └──► (Non-Retryable Exception: Poison Pill / Malformed JSON) ──────────────────► [orders.DLT Topic]
                                                                                                  │
                                                                                                  ▼
                                                                                         [DltOrderConsumer]
                                                                                      (Extract Exception Headers)
```

---

### 5. DLT Exception Headers & DLT Listener Pattern

When Spring Kafka's `DeadLetterPublishingRecoverer` forwards a failed record to `orders-dlt`, it attaches special diagnostic metadata as **Kafka Record Headers**:
- `kafka_dlt-original-topic`: The source topic where the failure occurred.
- `kafka_dlt-original-offset`: The offset number of the failing record.
- `kafka_dlt-exception-message`: The exact exception message/cause string.
- `kafka_dlt-exception-stacktrace`: The complete exception stack trace.

By creating a dedicated `DltOrderConsumer` (`@KafkaListener(topics = "orders-dlt")`), we extract these `@Header` parameters to log alerts or store failed events in an Audit/Dead-Letter DB for manual replay.

---

### 6. Consumer Concurrency & Graceful Shutdown

1. **`setConcurrency(3)`:** Configures Spring Kafka to spawn **3 parallel listener threads (worker containers)** per consumer instance. This enables true parallel partition consumption when the topic has multiple partitions.
2. **`setShutdownTimeout(10000L)` (Graceful Shutdown):** When the application is stopping (e.g. during a Kubernetes deployment or SIGTERM signal), Spring Kafka waits up to **10 seconds** for in-flight records to complete processing and commit offsets cleanly before shutting down, preventing message loss or duplicate re-processing.

---

## Interactive Q&A (Sprint Discussion Notes)

### ❓ Question 1: JSON vs Avro Efficiency
- **Discussion:** Why choose Avro/Schema Registry over JSON in production?
- **Key Insight:** Avro uses binary serialization without repeating field names in payloads. Network payload size drops by **60-80%**, saving massive bandwidth and disk I/O. Schema Registry enforces backward compatibility at publish time.

### ❓ Question 2: Idempotent Consumer Design
- **Discussion:** How to prevent duplicate database writes on the Consumer side?
- **Key Insight:** Use DB `PRIMARY KEY` / `UNIQUE INDEX` or a `processed_events` table log so duplicate events trigger a harmless constraint skip (`ON CONFLICT DO NOTHING`).

### ❓ Question 3: Poison Pills vs Temporary Outages
- **Discussion:** How should a Consumer handle temporary DB outages vs corrupted messages?
- **Key Insight:** Temporary outages use **Exponential Backoff** retries. Corrupted messages (Poison Pills) should have **0 Retries** and be routed directly to a Dead Letter Topic (DLT) to avoid blocking healthy messages. A dedicated DLT Replay Service inspects and reprocesses them.

### ❓ Question 4: Automatic Idempotence & `retries=MAX`
- **Discussion:** Does `enable.idempotence=true` handle PID and Sequence Numbers automatically? Why set `retries=MAX`?
- **Key Insight:** Yes, PID and Sequence Number management is 100% automatic inside the Kafka client/broker. `retries=Integer.MAX_VALUE` allows continuous retries during broker leader elections, bounded safely by `delivery.timeout.ms` (2 minutes).

### ❓ Question 5: Manual AckMode & DLT Listener Offset Commits
- **Discussion:** Why were DLT messages re-processed on every application restart even after being logged?
- **Key Insight:** When using `AckMode.MANUAL_IMMEDIATE`, Spring Kafka requires *every* `@KafkaListener` (including `DltOrderConsumer`) to explicitly call `acknowledgment.acknowledge()`. Without `acknowledgment.acknowledge()` inside `DltOrderConsumer`, the offsets for `orders-dlt` topic were never committed to Kafka, causing `dlt-order-group` to re-read all DLT messages from the beginning upon every restart.

---

## Implementation Roadmap (Step-by-Step)

- [x] **Step 1:** Theoretical Foundation, Cause-and-Effect Analysis & Documentation (`docs/sprint-12-production-improvements.md`)
- [x] **Step 2:** Configure Producer Hardening (`producer-service` `application.yaml`: `enable.idempotence=true`, `acks=all`)
- [x] **Step 3:** Configure Consumer Classified Error Handler & DLT in `consumer-service` (`DefaultErrorHandler`, `ExponentialBackOff`, Non-Retryable Exceptions)
- [x] **Step 4:** Build Dedicated DLT Consumer (`DltOrderConsumer.java`) & Exception Header Extractor
- [x] **Step 5:** Configure Listener Concurrency (3 threads) & Graceful Shutdown (10s timeout)
- [x] **Step 6:** Verification & Testing (Valid events, DB retry, Poison Pill DLT routing)

---

## Troubleshooting & Real-World Gotchas (Karşılaşılan Sorunlar ve Çözümleri)

### 1. Manuel Commit (`MANUAL_IMMEDIATE`) Kullanırken DLT Mesajlarının Her Restart'ta Tekrar Okunması
- **Sorunun Kök Nedeni:** 
  1. `@KafkaListener` (`OrderConsumer`) içindeki `try-catch` bloğunun ham hatayı (`JsonParseException`) yakalayıp `new RuntimeException(e)` olarak sarmalaması yüzünden Spring Kafka'nın `addNotRetryableExceptions` kuralı eşleşmiyordu.
  2. `AckMode.MANUAL_IMMEDIATE` kullanıldığında, `DefaultErrorHandler` DLT'ye fırlatsa dahi koddaki `acknowledgment.acknowledge()` satırına ulaşılamadığı için `orders` topic offset'i commit edilemiyordu (`LAG = 2` olarak çakılı kalıyordu).
  3. `DltOrderConsumer` listener metodunda `Acknowledgment acknowledgment` parametresi eksik olduğu için `orders-dlt` offset'leri de commit edilmiyordu.
- **Nasıl Çözüldü?**
  1. `OrderConsumer` içindeki gereksiz `try-catch` sarmalayıcısı kaldırıldı (`throws Exception`).
  2. `KafkaConsumerConfig` sınıfına `errorHandler.setAckAfterHandle(true)` eklendi. Böylece Manuel Commit modunda bile DLT'ye kurtarılan zehirli mesajların offset'i otomatik commit edildi.
  3. `DltOrderConsumer` metoduna `Acknowledgment` eklenip `acknowledgment.acknowledge()` ile DLT offset'i onaylandı.

---

### 2. Üç Temel Kafka Mimari Sorusu ve Çözümleri

#### A. Zehirli veri neden `orders` topic'ine de yazılabildi?
- **Neden?** Kafka Broker "Dumb Pipe" (Aptal Boru) mantığıyla çalışır. Gelen her `byte[]` dizisini denetlemeden diske yazar. Veri validasyonu yapmaz.
- **Çözüm:** **Schema Registry & Avro** sayesinde Producer bozuk mesajı Kafka'ya basamadan daha compile/publish anında engellenir.

#### B. Bazı veriler neden hem `orders` hem `orders-dlt` topic'inde var?
- **Neden?** Kafka log'ları değiştirilemez ve silinemez (Immutable). Orijinal mesaj `orders` topic'inde kalır. DLT bir "Hata Havuzu" olduğu için `DefaultErrorHandler` mesajı silmez, bir kopyasını `orders-dlt` topic'ine fırlatır.

#### C. Zehirli veri neden `orders-dlt` topic'ine peş peşe yazıldı?
- **Neden?** Offset commit atılamayan dönemde Consumer servisi her restart edildiğinde mesaj `orders` topic'inden baştan okunup tekrar tekrar DLT'ye fırlatıldığı için kopyaları peş peşe yazılmıştı. `setAckAfterHandle(true)` sonrası offset commit edildi ve bu sorun tamamen çözüldü.
