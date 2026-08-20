# Sprint 11 - Monitoring & Observability in Kafka & EDA

## Goal

Implement end-to-end **Monitoring & Observability** for our Event-Driven Platform across Spring Boot Producers/Consumers, Kafka Connect, Kafka Streams, and PostgreSQL using **Spring Boot Actuator**, **Micrometer**, **Prometheus**, and **Grafana**.

Understand why observability in asynchronous, event-driven architectures differs fundamentally from synchronous REST applications, master core Kafka metrics (Consumer Lag, Rebalance rates, Latency, Buffer Pool), and build a live monitoring dashboard.

---

## Architecture Diagram: Observability Flow

```text
[Spring Boot Services / Kafka Clients] 
 (Producer / Consumer / Streams)
         │
         ▼ 1. Collects Metrics via Common API
    [Micrometer]
         │
         ▼ 2. Exposes Prometheus Format Metrics
  [Spring Boot Actuator: /actuator/prometheus]
         │
         ▲ 3. HTTP Pull / Scrape (Every 15 seconds)
  [Prometheus Server (Docker)]
         │
         ▼ 4. PromQL Queries & Dashboard Panels
  [Grafana Dashboard (Docker)]
```

---

## Theoretical Concepts & Cause-and-Effect Scenarios

### 1. Why Observability in Event-Driven Architecture (EDA)?

In traditional synchronous REST architectures, errors are immediate: an HTTP 500 error is returned and logged. 

In **Event-Driven Architectures (EDA)**, the client receives `202 Accepted` immediately, but the business transaction continues asynchronously across:
$$\text{Producer} \rightarrow \text{Outbox DB} \rightarrow \text{Debezium CDC} \rightarrow \text{Kafka Topic} \rightarrow \text{Kafka Streams} \rightarrow \text{Consumer}$$

If a message gets stuck or delayed by 3 hours, no HTTP 500 is thrown! Observability provides the "flashlight in the dark" through the **3 Pillars**:
1. **Metrics:** Quantitative numeric series over time (e.g., Consumer Lag, Throughput, Error Rate).
2. **Logs:** Contextual textual event records (e.g., Exception stack trace with `orderId`).
3. **Traces:** End-to-end request lifecycle across distributed service boundaries using `TraceID`.

---

### 2. The Core Observability Pillars in Kafka

#### A. Consumer Lag (Tüketici Gecikmesi)
- **Log End Offset (LEO):** The highest offset written to a Kafka partition.
- **Committed Offset:** The last offset successfully processed and committed by the Consumer Group.
- **Consumer Lag:** $\text{Consumer Lag} = \text{LEO} - \text{Committed Offset}$.
- **Cause-and-Effect Risk:** If Consumer Lag grows monotonically, data becomes stale. If Lag exceeds Kafka's retention time (`log.retention.hours`), unprocessed messages are deleted from disk, causing **Silent Data Loss**.

#### B. Rebalance Storms (Yeniden Dengeleme Fırtınası)
- `heartbeat.interval.ms`: Background thread ping to broker ("I am alive").
- `max.poll.interval.ms`: Max time between consecutive `poll()` calls ("I am processing work").
- **Cause-and-Effect Risk:** If a Consumer takes longer than `max.poll.interval.ms` (e.g., due to slow DB calls), the Broker assumes the thread crashed and kicks it out of the group. The partition is reassigned (Rebalance). When the old consumer finishes, its commit fails (`CommitFailedException`). The new consumer re-reads the same batch, also times out, triggering a **Rebalance Storm**.

#### C. Distributed Tracing & Kafka Record Headers
- Monolithic systems pass `TraceID` in HTTP headers.
- In Kafka, trace context is propagated using **Kafka Record Headers**.
- For Outbox Pattern + Debezium CDC, a dedicated `trace_id` or `tracestate` column in the database table is mapped to Kafka Headers by Debezium's Event Router SMT.

---

## Interactive Q&A (Sprint Discussion Notes)

### ❓ Question 1: Partition vs Consumer Scaling Limit
> **Scenario:** On Black Friday, `orders` topic's Consumer Lag is spiking. You decide to scale out the Consumer Kubernetes deployment from 3 pods to 10 pods. However, `orders` topic has **4 partitions**. Will scaling to 10 pods solve the lag? Why?

- **Answer:** **No, it will NOT solve the lag.**
- **Reason:** In Kafka, a single partition within a Consumer Group can only be assigned to **one consumer instance at a time**. With 4 partitions, at most 4 consumers can actively read data in parallel. The remaining 6 consumers will sit completely **idle (atıl)**. To utilize 10 consumers, topic partition count must first be increased to at least 10.

---

### ❓ Question 2: Resolving Rebalance Storms
> **Scenario:** How can we prevent Rebalance Storms when processing heavy batches or slow external calls in a Consumer?

- **Answer:** 2 Primary Solutions:
  1. **Configuration Fix:** Increase `max.poll.interval.ms` (e.g., from 5 mins to 15 mins) and reduce `max.poll.records` (e.g., process 50 records per poll instead of 500).
  2. **Architecture Fix (Worker Threads):** Hand off incoming records from the `poll()` thread to a background worker thread pool (`ExecutorService`), allowing the main thread to poll Kafka continuously while background workers process the payload.

---

### ❓ Question 3: Propagating TraceID via Outbox Pattern
> **Scenario:** Since Producer writes to PostgreSQL `outbox` table rather than directly to Kafka, how does `TraceID` reach Kafka headers?

- **Answer:** Add a `trace_parent` / `trace_id` column to the `outbox` table. When Debezium CDC reads the WAL, Debezium Outbox Event Router SMT extracts this column and automatically converts it into a Kafka Record Header.

---

## Key Kafka Client Metrics Reference

### 📤 Producer Metrics
- `record-send-rate`: Number of records sent per second (Throughput).
- `request-latency-avg`: Average time (ms) spent waiting for Kafka broker ACK response.
- `bufferpool-wait-ratio`: **CRITICAL.** Fraction of time producer threads spent waiting for memory buffer pool allocation. If $\rightarrow 1.0$, Producer RAM is full and will throw `TimeoutException` / OOM.

### 📥 Consumer Metrics
- `records-lag-max`: **CRITICAL.** Maximum lag in messages across all assigned partitions.
- `fetch-rate`: Number of fetch requests sent to Kafka brokers per second. (Low rate + High lag = Consumer bottlenecked by internal processing).
- `rebalance-rate`: Number of consumer group rebalances per second. (Should be 0 in steady state).

### 🔄 Kafka Streams Metrics
- `process-rate`: Number of records processed per second across the topology.
- `state-store-active-memtable-bytes`: Memory used by local RocksDB state stores. High usage requires heap/off-heap memory tuning to prevent `OOMKilled`.

---

## Architecture Components & Tools

| Tool | Purpose | Role in Architecture |
| :--- | :--- | :--- |
| **Spring Boot Actuator** | Exposes application internals via HTTP endpoints (`/actuator/health`, `/actuator/metrics`). | Provides the data exposure layer. |
| **Micrometer** | Standard Java metric collection facade (abstraction layer). | Converts application metrics to Prometheus metric format (`/actuator/prometheus`). |
| **JMX (Java Management Extensions)** | Low-level JVM and Kafka native client MBeans exposure. | Emits raw Kafka client metrics (Lag, Send Rate, Latency). |
| **Prometheus** | Time-Series Database (TSDB) & Scraper. | Periodically pulls (scrapes) metrics every 15s from `/actuator/prometheus`. |
| **Grafana** | Visualization & Dashboard UI. | Queries Prometheus via PromQL to render real-time charts, graphs, and alerts. |

---

## Implementation Roadmap (Step-by-Step)

- [x] **Step 1:** Theoretical Foundation, Cause-and-Effect Analysis & Documentation (`docs/sprint-11-monitoring-observability.md`)
- [x] **Step 2:** Configure Spring Boot Services (`producer-service`, `consumer-service`, `stream-service`) with Actuator & Micrometer Prometheus
- [x] **Step 3:** Configure Prometheus & Grafana in `docker-compose.yml`
- [x] **Step 4:** Write `prometheus.yml` scrape configuration file
- [ ] **Step 5:** Boot cluster, verify `/actuator/prometheus` endpoints, scrape targets, and build Grafana Dashboard

---

## Sequential Setup Guide (Sıralı Kurulum Rehberi)

Sistemi sıfırdan kurmak istediğinde izlemen gereken adım adım sıralı yönerge:

### Adım 1: Spring Boot Servislerine Metrik Bağımlılıklarını Ekleme

Her bir mikroservisin (`producer-service`, `consumer-service`, `stream-service`) bağımlılıklarına **Spring Boot Actuator** ve **Micrometer Prometheus** eklenmelidir.

- **Maven (`pom.xml` - `producer-service`):**
  ```xml
  <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
  </dependency>
  <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
  </dependency>
  ```

- **Gradle (`build.gradle` - `consumer-service` & `stream-service`):**
  ```groovy
  implementation 'org.springframework.boot:spring-boot-starter-actuator'
  implementation 'io.micrometer:micrometer-registry-prometheus'
  // Not: stream-service için HTTP endpoint açabilmesi adına 'org.springframework.boot:spring-boot-starter-web' şarttır.
  ```

---

### Adım 2: `application.yaml` Konfigürasyonu Yapma

Servislerin `/actuator/prometheus` endpoint'ini dış dünyaya açması ve metrikleri uygulama adıyla etiketlemesi için tüm servislerin `src/main/resources/application.yaml` dosyasına eklenmelidir:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info, metrics, prometheus
  endpoint:
    health:
      show-details: always
    prometheus:
      enabled: true
  metrics:
    tags:
      application: ${spring.application.name}
```

- **`exposure.include: prometheus`:** Prometheus'un okuyacağı `/actuator/prometheus` yolunu aktif eder.
- **`metrics.tags.application`:** Prometheus üzerindeki grafikte hangi verinin hangi servise ait olduğunu ayırt etmeyi sağlar.

---

### Adım 3: Prometheus Konfigürasyon Dosyasını Hazırlama (`prometheus/prometheus.yml`)

Proje kökünde `prometheus/prometheus.yml` adında bir dosya oluşturulur. Bu dosya Prometheus'un nereleri tarayacağını (scrape target) belirler:

```yaml
global:
  scrape_interval: 15s # Her 15 saniyede bir metrik çek

scrape_configs:
  - job_name: 'producer-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8080']

  - job_name: 'consumer-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8081']

  - job_name: 'stream-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['host.docker.internal:8082']
```

---

### Adım 4: Docker Compose Servislerini Ekleme (`docker-compose.yml`)

`docker-compose.yml` dosyasına Prometheus ve Grafana konteynerleri eklenir:

```yaml
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    extra_hosts:
      - "host.docker.internal:host-gateway"

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
```

---

### Adım 5: Doğrulama ve Grafana Konfigürasyonu

1. Servisleri ve konteynerleri başlat:
   `docker-compose up -d`
2. Tarayıcıda `/actuator/prometheus` endpoint'lerinin çalıştığını doğrula:
   - `http://localhost:8080/actuator/prometheus` (Producer)
   - `http://localhost:8081/actuator/prometheus` (Consumer)
   - `http://localhost:8082/actuator/prometheus` (Stream)
3. Prometheus UI (`http://localhost:9090`) -> **Status -> Targets** menüsünden tüm servislerin `UP` olduğunu kontrol et.
4. Grafana UI (`http://localhost:3000`) -> Kullanıcı Adı: `admin` / Şifre: `admin` ile giriş yap.
5. Data Source olarak `http://prometheus:9090` ekle ve Dashboard panellerini bağla.

---

## Troubleshooting & Common Gotchas (Karşılaşılan Sorunlar ve Çözümleri)

### 1. `Identifier of entity '...Order' must be manually assigned before calling 'persist()'`

- **Hata Tanımı:** Consumer servisi başlatıldığında `Error: Identifier of entity 'com.eventplatform.consumerservice.entity.Order' must be manually assigned before calling 'persist()'` hatası verir ve `Record in retry and not yet recovered` diyerek sonsuz döngüye girer.
- **Neden Yaşandı?** 
  - Kafka `orders` topic'indeki mesajın JSON yapısında ID alanı `"orderId"` olarak değil; Debezium CDC/Outbox formatına uygun olarak `"id"`, `"order_id"` veya `"aggregate_id"` şeklinde gelmektedir.
  - Consumer servisindeki `Order` entity sınıfında Jackson eşleştirme anotasyonları olmadığı için Jackson `orderId` alanını okuyamayıp `null` atamaktadır.
  - JPA/Hibernate `@Id` birincil anahtarı `null` olan nesneyi veritabanına kaydedemediği için persist hatası fırlatır.
- **Nasıl Çözüldü?**
  1. `Order.java` entity'sine esnek JSON okuması için `@JsonAlias({"id", "order_id", "aggregate_id", "aggregateId"})` ve `@JsonIgnoreProperties(ignoreUnknown = true)` eklendi.
  2. `OrderConsumer.java` listener metoduna `orderId`'nin boş gelme ihtimaline karşı `if (order.getOrderId() == null) order.setOrderId(System.currentTimeMillis());` emniyet kontrolü eklendi.

---

## Personal Note / Self-Study Reminder (Kişisel Çalışma Notu)

- [ ] **Araştırma Konusu (Big Data & JVM Metrikleri):** Grafana'da gördüğüm Spring Boot & JVM grafiklerinin (Heap Used, Eden Space, Old Gen, Metaspace, GC Stop-The-World, HikariCP Connection Pool) ne anlama geldiğini ve Big Data / Streaming dünyasında nasıl yorumlanması gerektiğini daha sonra detaylı araştır.



