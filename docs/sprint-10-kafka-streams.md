# Sprint 10 - Kafka Streams (Real-Time Event Processing)

## Goal

Build a real-time stream processing engine (`stream-service`) using **Spring Boot** and **Kafka Streams**. 

Learn how to process, enrich, filter, and aggregate event streams directly inside Kafka in real time, moving beyond simple producer-consumer patterns into event-driven stream processing architecture.

---

## What We Built

- Configured a dedicated `stream-service` microservice with Spring Kafka Streams (`@EnableKafkaStreams`).
- Created data models for incoming raw events (`OrderEvent`) and enriched downstream events (`ProcessedOrderEvent`).
- Built a multi-step Kafka Streams topology ([`OrderStreamTopology.java`]):
  1. **Stateless Filtering:** Filtered invalid and cancelled order events.
  2. **Stateless Enrichment:** Transformed raw orders into enriched business events with category classification and processing timestamps.
  3. **Event Routing:** Published enriched events to the `processed-orders` output topic.
  4. **Stateful Aggregation:** Grouped orders by product key and computed real-time order counts per product using a local **RocksDB State Store**.
  5. **Changelog Sink:** Streamed live aggregation results to the `product-counts` topic.

---

## Theoretical Concepts & Architecture

### What is Kafka Streams?

Kafka Streams is a client library for building real-time stream processing applications. 

Unlike external stream processing frameworks (e.g. Apache Flink or Apache Spark Streaming), Kafka Streams runs **embedded inside your Java / Spring Boot application**. You do not need to maintain a separate streaming cluster.

### Core Concepts

#### 1. StreamsBuilder & Topology
A **Topology** is a Directed Acyclic Graph (DAG) of stream processing nodes:
- **Source Node:** Reads records from an input Kafka topic.
- **Processor Node:** Transforms, filters, or aggregates records.
- **Sink Node:** Writes records to an output Kafka topic.

#### 2. KStream vs KTable
- **`KStream` (Event Stream):** Represents an append-only stream of facts. Every incoming record is treated as an independent event (*Insert semantics*).
  - *Example:* "Order #101 created", "Order #102 created".
- **`KTable` (Changelog / State Table):** Represents the current state of data by key (*Upsert / Update semantics*). Each new record with key *K* overwrites the previous value for *K*.
  - *Example:* "Product 'Laptop' -> total count: 5".

#### 3. Stateless vs Stateful Operations
- **Stateless Operations (`filter`, `mapValues`, `selectKey`, `peek`):**
  - Process each record independently without remembering past records.
  - Do not require disk storage or memory state stores.
- **Stateful Operations (`groupByKey`, `count`, `aggregate`, `windowedBy`):**
  - Require state memory across multiple events.
  - Automatically persist state into a local **RocksDB** embedded key-value store.
  - Kafka automatically creates a **Changelog Topic** for the state store to ensure zero data loss on node crashes.

#### 4. Serdes (Serializer / Deserializer)
Kafka stores keys and values as raw byte arrays (`byte[]`). A **Serde** converts binary bytes into Java objects and vice-versa.
- In our topology, we used `Serdes.String()` for String payloads and Jackson `ObjectMapper` for JSON conversion.

---

## Stream Topology Flow Diagram

```
Input Topic: "orders"
         │
         ▼
 ┌───────────────┐
 │ filter(...)   │ (Removes null & CANCELLED orders)
 └───────┬───────┘
         │
         ├───► ┌──────────────────┐
         │     │ mapValues(...)   │ (Enriches event with category & timestamp)
         │     └────────┬─────────┘
         │              │
         │              ▼
         │     Output Topic: "processed-orders"
         │
         ▼
 ┌───────────────┐
 │ selectKey(...)│ (Re-keys stream by Product Name)
 └───────┬───────┘
         │
         ▼
 ┌────────────────┐
 │ groupByKey()   │
 └───────┬────────┘
         │
         ▼
 ┌───────────────────────────┐
 │ count() + RocksDB Store   │ (Stateful aggregation per product)
 └───────┬───────────────────┘
         │
         ▼
 ┌────────────────┐
 │ toStream()     │ (Converts KTable changelog back to KStream)
 └───────┬────────┘
         │
         ▼
 Output Topic: "product-counts"
```

---

## Kafka Topics Breakdown (What Are These Topics?)

Kafka Streams mimarisinde konular (topics) iki ana kategoriye ayrılır: **Uygulama Çıkış Konuları (User-Defined Output Topics)** ve **Kafka Streams Dahili Konuları (Internal Topics)**.

### 1. Uygulama Çıkış Konuları (User-Defined Output Topics)

- **`processed-orders` (Enriched Business Events):**
  - **Amaç:** Filtrelenmiş ve kategori (`ELECTRONICS`, `BOOKS` vb.) ile işlenme zamanı (`processedAt`) eklenerek zenginleştirilmiş sipariş olaylarının yayınlandığı çıkış konusudur.
  - **Nasıl Oluşur?** Kodumuzda `.to("processed-orders")` metodu ile açıkça tanımlanmıştır.
  - **İçerik:** `{"orderId":101, "product":"Laptop", "category":"ELECTRONICS", "processedAt":"2026-08-20T..."}`

- **`product-counts` (Live Aggregation Stream):**
  - **Amaç:** Ürün bazındaki canlı sipariş sayılarının (`KTable` durum güncellemeleri) dış dünyaya akış olarak sunulduğu konudur.
  - **Nasıl Oluşur?** Kodumuzda `productOrderCounts.toStream().to("product-counts")` ile tanımlanmıştır.
  - **İçerik:** Key: `"Gaming Laptop"`, Value: `{"totalOrders": 3}`

---

### 2. Kafka Streams Dahili Konuları (Internal Topics)

Bu konular kodda bizim tarafımızdan elle oluşturulmaz; Kafka Streams tarafından `application-id` (`stream-service-group`) ön ekiyle otomatik olarak yönetilir.

- **`stream-service-group-product-counts-store-changelog` (Internal State Store Backup):**
  - **Amaç (Hata Toleransı & State Restoration):** Kafka Streams, gruplama ve sayım verisini yerel **RocksDB** veritabanında tutar. Ancak sunucu çökerse, uygulama yeniden başlarsa veya başka bir makineye taşınırsa yerel disktaki veri kaybolabilir. 
  - **Nasıl Çalışır?** RocksDB üzerinde yapılan her sayısal güncelleme anında bu `-changelog` konusuna bir yedek olarak yazılır. Uygulama çöktüğünde veya yeniden başladığında, bu konudaki kayıtları baştan okuyarak yerel RocksDB önbelleğini **0 veri kaybı** ile saniyeler içinde tekrar inşa eder.
  - **İsim Kalıbı:** `<application-id>-<state-store-name>-changelog`

- **`stream-service-group-product-counts-store-repartition` (Internal Repartition / Shuffle Topic):**
  - **Amaç (Partition Hizalaması / Re-keying):** Kafka'da mesajlar partition'lara mesajın **Key** değerine göre dağıtılır. İlk başta gelen mesajların key'i `null` veya `orderId` idi. Biz `.selectKey()` yaparak mesaj anahtarını `product` (ürün adı) olarak değiştirdik.
  - **Nasıl Çalışır?** Gruplama (`groupByKey()`) ve sayım (`count()`) işlemlerinin doğru çalışabilmesi için, **aynı ürüne ait tüm mesajların Kafka'daki aynı partition'a düşmesi şarttır**. Kafka Streams, mesaj anahtarını değiştirdiğimizi anladığı anda veriyi yeniden hizalamak (re-shuffle) için arka planda otomatik olarak bu `-repartition` konusunu oluşturur ve mesajları ürün adına göre doğru partition'lara yeniden dağıtır.
  - **İsim Kalıbı:** `<application-id>-<state-store-name>-repartition`

---

## Implementation Details (What & Why)

### 1. Configuration & Setup
- **[`build.gradle`]:** Added `org.apache.kafka:kafka-streams` and `com.fasterxml.jackson.core:jackson-databind`.
- **[`KafkaStreamsConfig.java`]:**
  - Used `@EnableKafkaStreams` so Spring Boot manages the lifecycle of the `KafkaStreams` instance automatically.
  - Configured `application-id`: `stream-service-group` (Acts as the Kafka Streams consumer group ID and state store namespace).

### 2. Topology Construction ([`OrderStreamTopology.java`])

```java
// Source: Dinlenen Konu
KStream<String, String> rawOrderStream = streamsBuilder.stream("orders");

// Stateless Filter: İptal edilmiş veya boş siparişleri ayıkla
KStream<String, String> validOrdersStream = rawOrderStream
        .filter((key, value) -> value != null && !value.trim().isEmpty());

// Stateless Map: Event Enrichment (Zenginleştirme)
KStream<String, String> processedOrdersStream = validOrdersStream.mapValues(value -> {
    OrderEvent order = objectMapper.readValue(value, OrderEvent.class);
    String category = resolveCategory(order.getProduct());
    ProcessedOrderEvent processed = new ProcessedOrderEvent(
        order.getOrderId(), order.getProduct(), category, Instant.now().toString()
    );
    return objectMapper.writeValueAsString(processed);
});

// Output Topic 1: Zenginleştirilmiş siparişler
processedOrdersStream.to("processed-orders");

// Stateful Re-Keying & Aggregation
KStream<String, String> productKeyedStream = validOrdersStream
        .selectKey((key, value) -> extractProduct(value));

KTable<String, Long> productOrderCounts = productKeyedStream
        .groupByKey()
        .count(Materialized.as("product-counts-store"));

// Output Topic 2: Canlı sipariş sayımları
productOrderCounts.toStream()
        .mapValues(count -> "{\"totalOrders\": " + count + "}")
        .to("product-counts");
```

---

## Problems Encountered & Solutions

### 1. `NoSuchBeanDefinitionException: ObjectMapper`
- **Issue:** Spring Boot core starter without Web/JSON did not automatically declare an `ObjectMapper` bean for dependency injection.
- **Solution:** Explicitly defined a `@Bean public ObjectMapper objectMapper()` in [`KafkaStreamsConfig.java`].

### 2. Gradle Toolchain & Invalid Dependencies
- **Issue:** `build.gradle` contained invalid artifact name `spring-boot-starter-kafka` and Spring Boot version `4.1.0`.
- **Solution:** Corrected Spring Boot version to `3.4.3`, replaced dependency with `org.springframework.kafka:spring-kafka`, and added `foojay-resolver` plugin to `settings.gradle`.

---

## Verification & Testing

1. Start Kafka Docker Cluster:
   ```bash
   docker-compose up -d
   ```
2. Run `stream-service`:
   ```bash
   ./gradlew.bat bootRun
   ```
3. Produce a test order to `orders` topic:
   ```json
   {"orderId": 101, "product": "Gaming Laptop"}
   ```
4. Expected Results:
   - **`processed-orders` Topic:** Receives enriched JSON `{"orderId":101,"product":"Gaming Laptop","category":"ELECTRONICS","processedAt":"..."}`.
   - **`product-counts` Topic:** Receives `{"totalOrders": 1}` for key `"Gaming Laptop"`.

---

## Key Lessons Learned

1. **Kafka Streams runs embedded inside your application**, making real-time stream processing easy to deploy without heavy streaming clusters.
2. **KStream is for event facts, KTable is for state updates.**
3. **Re-keying (`selectKey`) is required** before grouping or aggregating when partitioning by a new key.
4. **RocksDB handles state locally**, and Kafka changelog topics back up the state automatically.
5. **Spring Kafka Streams (`@EnableKafkaStreams`)** seamlessly integrates topology definitions into the Spring application lifecycle.
