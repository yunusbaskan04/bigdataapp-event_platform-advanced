Sprint 07 - Idempotency, Transactions & Outbox Pattern
Goal

Understand how distributed event-driven systems prevent duplicate processing, ensure reliable event publishing, and maintain consistency between databases and Kafka.

What We Learned
Learned why duplicate messages are unavoidable in distributed systems.
Understood the difference between Idempotent Consumer and Idempotent Producer.
Explored how Kafka prevents duplicate writes using Producer IDs and Sequence Numbers.
Learned the limitations of Kafka Transactions.
Understood why database transactions and Kafka transactions are independent.
Learned the motivation behind the Transactional Outbox Pattern.
Understood how Debezium captures database changes using PostgreSQL WAL.
Connected today's concepts with the future Kafka Connect and Debezium implementation.
Architecture Evolution
Traditional Event Publishing
Client
   │
   ▼
Order Service
   │
   ├────────► PostgreSQL
   │
   └────────► Kafka

Problem:

Database ✔
Kafka ❌

Application crashes after saving the database but before publishing the event.

This creates inconsistent distributed state.

Transactional Outbox Pattern
Client
   │
   ▼
Order Service
   │
   ▼
Database Transaction
   │
   ├────────► Orders Table
   │
   └────────► Outbox Table
                    │
                    ▼
             PostgreSQL WAL
                    │
                    ▼
               Debezium CDC
                    │
                    ▼
              Kafka Topic

Now both business data and integration events are committed inside the same database transaction.

Key Concepts Learned
Idempotent Consumer

Consumers operating with At-Least-Once Delivery must expect duplicate messages.

Instead of preventing duplicates, the consumer should safely process the same message multiple times.

Example:

Message Delivered
        │
        ▼
Database Insert
        │
Crash before Offset Commit
        │
Kafka Redelivers Message
        │
Database State Remains Correct

Our project already demonstrates this behavior because:

orderId is the primary key.
Duplicate deliveries do not create duplicate database rows.
Idempotent Producer

Producer retries may generate duplicate writes.

Example:

Producer
    │
Send Record
    │
Broker Stores Record
    │
ACK Lost
    │
Producer Retries

Without Idempotent Producer:

Duplicate Record

With:

enable.idempotence=true

Kafka assigns:

Producer ID (PID)
Sequence Number

Kafka ignores duplicated sequence numbers and writes the record only once.

Kafka Transactions

Idempotent Producer prevents duplicate writes.

However, it cannot solve partial failures such as:

Database Commit ✔


Kafka Publish ❌

Kafka Transactions make the following operations atomic:

Produce Records
        +
Offset Commit

Important:

Kafka Transactions do not include database transactions.

Database Transaction vs Kafka Transaction

One of the most important lessons of this sprint.

Database Transaction


Orders Table
      +
Outbox Table

is completely different from

Kafka Transaction


Produce Event
      +
Offset Commit

These two transaction systems are independent.

Transactional Outbox Pattern

Instead of publishing directly to Kafka:

save(order);


kafkaTemplate.send(...);

the application stores an event inside an Outbox table.

Orders Table
        +
Outbox Table

Both inserts belong to the same database transaction.

Later, another component publishes events to Kafka.

Debezium & CDC

Debezium does not continuously execute SQL queries.

It reads PostgreSQL's Write Ahead Log (WAL).

Database Commit
        │
        ▼
PostgreSQL WAL
        │
        ▼
Debezium
        │
        ▼
Kafka

This approach is called Change Data Capture (CDC).

Advantages:

Near real-time
No polling
Minimal database overhead
Why Outbox Instead of Reading Business Tables?

Business tables contain application state.

Orders
Payments
Customers

Outbox contains integration events.

OrderCreated
PaymentCompleted
OrderCancelled

Debezium publishes only the events intended for other services.

Big Picture
Client
   │
HTTP Request
   │
   ▼
Order Service
   │
Database Transaction
   │
├──────────────┐
▼              ▼
Orders      Outbox
                │
                ▼
        PostgreSQL WAL
                │
                ▼
           Debezium CDC
                │
                ▼
          Kafka Topic
                │
        ┌───────┴────────┐
        ▼                ▼
Inventory Service   Email Service
Lessons Learned
Distributed systems must always expect duplicate delivery.
Idempotent Consumer protects the business layer.
Idempotent Producer protects Kafka from duplicate writes.
Kafka Transactions cannot make database operations atomic.
Database consistency and event consistency require different approaches.
Transactional Outbox Pattern guarantees reliable event publishing.
Debezium reads WAL instead of polling database tables.
CDC provides an efficient bridge between relational databases and Kafka.
Next Sprint

In the next sprint we will implement the concepts learned today.

Topics:

Kafka Connect
Debezium Installation
PostgreSQL Logical Replication
Outbox Table Implementation
Debezium Connector Configuration
Automatic Event Publishing
End-to-End CDC Pipeline