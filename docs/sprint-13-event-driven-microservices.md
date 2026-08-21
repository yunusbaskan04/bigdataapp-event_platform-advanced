# Sprint 13 - Event-Driven Microservices (Multi-Consumer Architecture & Fan-Out Pattern)

## 📌 Executive Summary
In previous sprints, `consumer-service` operated as a single consumer group (`order-group`) persisting orders back to PostgreSQL. In production enterprise architectures, event producers publish **Domain Events** (e.g. `OrderCreatedEvent`), and multiple independent microservices subscribe to the exact same event stream concurrently to execute domain-specific business logic without coupling.

Sprint 13 transforms our single-consumer pipeline into a **Multi-Consumer Fan-Out Event-Driven Architecture**.

---

## 🎯 Architectural Goals & Key Concepts

### 1. The Fan-Out Pattern (Publish-Subscribe)
- **Concept:** A single event published to Kafka topic `orders` is delivered to **multiple independent Consumer Groups** simultaneously.
- **Why?** Decouples producers from consumers. The `producer-service` does not know or care how many downstream microservices consume the event. Adding a new service (e.g., Fraud Detection or Marketing) requires **zero code changes** in `producer-service`.

```text
                               ┌────────────────────────────────────────────────────────┐
                               │                    Kafka Topic: orders                 │
                               └───────────────────────────┬────────────────────────────┘
                                                           │
               ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
               ▼                                           ▼                                           ▼
   [notification-group]                         [inventory-group]                           [analytics-group]
   NotificationConsumer                         InventoryConsumer                           AnalyticsConsumer
   (Simulates Email/SMS)                        (Simulates Stock Deduction)                 (Simulates Metrics Logging)
```

---

### 2. Consumer Group Isolation
- **`notification-group`:** Consumes `orders` -> Sends simulated customer email/SMS.
- **`inventory-group`:** Consumes `orders` -> Simulates inventory stock deduction in warehouse database.
- **`analytics-group`:** Consumes `orders` -> Simulates event streaming to Data Warehouse / ElasticSearch.

Each consumer group maintains its **own independent offsets** in Kafka (`__consumer_offsets`). If one consumer service goes down or experiences backpressure, the others continue processing events at full speed.

---

### 3. Domain Events Structure
Events emitted contain domain metadata allowing consumers to route logic:
```json
{
  "eventType": "ORDER_CREATED",
  "orderId": 101,
  "product": "Gaming Laptop",
  "quantity": 1,
  "timestamp": 1740000000
}
```

---

## ❓ Interactive Q&A (Sprint Discussion Notes)

### ❓ Question 1: Consumer Group Isolation
- **Discussion:** If 3 services belong to 3 different `group.id`s (`notification-group`, `inventory-group`, `analytics-group`), does Kafka duplicate the message 3 times?
- **Key Insight:** Kafka Broker maintains **one single physical copy** of the message on disk. It delivers references to that message to each active Consumer Group independently. Memory and network overhead are minimal.

### ❓ Question 2: Downstream Failure Isolation
- **Discussion:** If `NotificationConsumer` throws an exception, does it affect `InventoryConsumer` or `AnalyticsConsumer`?
- **Key Insight:** **No.** Consumer Groups are 100% isolated. A failure or retry loop in `notification-group` has zero impact on `inventory-group` or `analytics-group`.

### ❓ Question 3: Centralized Error Handling & DLT Inheritance (Merkezi DLT Mirası)
- **Discussion:** Yeni yazdığımız `NotificationConsumer`, `InventoryConsumer` ve `AnalyticsConsumer` sınıflarının içinde hiç DLT kodu yazmadığımız halde, hata alınca zehirli mesajlar DLT'ye nasıl fırlatıldı?
- **Key Insight (Fabrika Analojisi):** 
  - `KafkaConsumerConfig` sınıfı içinde bir **`kafkaListenerContainerFactory` (Konteyner Fabrikası)** tanımladık ve bu fabrikaya `factory.setCommonErrorHandler(errorHandler)` diyerek **Merkezi DLT ve Error Handler** monte ettik.
  - Spring Boot projede yazılan tüm `@KafkaListener` dinleyicilerini (`NotificationConsumer`, `InventoryConsumer`, `AnalyticsConsumer`) **bu tek fabrikadan üretir.**
  - Dolayısıyla biz yeni bir Consumer sınıfı yazdığımızda, o sınıf DLT ve Retry yeteneklerini **bu fabrikadan otomatik miras alır.** Her yeni servise tek tek DLT kodu yazmamıza gerek kalmaz.

---

## 🚀 Implementation Roadmap

- [x] **Step 1:** Theoretical Overview & Design Documentation (`docs/sprint-13-event-driven-microservices.md`)
- [x] **Step 2:** Create `NotificationConsumer.java` (`groupId = "notification-group"`)
- [x] **Step 3:** Create `InventoryConsumer.java` (`groupId = "inventory-group"`)
- [x] **Step 4:** Create `AnalyticsConsumer.java` (`groupId = "analytics-group"`)
- [x] **Step 5:** End-to-End Fan-Out Verification & Multi-Consumer Console Output Inspection
