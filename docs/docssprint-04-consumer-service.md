# Sprint 04 - Spring Boot Consumer

## Goal

Build a Spring Boot consumer application that listens to Kafka topics and processes incoming events.

---

## What We Built

- Created a separate `consumer-service` microservice.
- Connected the service to Apache Kafka.
- Implemented the first Kafka Consumer using `@KafkaListener`.
- Successfully consumed messages sent by `producer-service`.

---

## Architecture

Client
↓
Producer Service
↓
Kafka Topic (orders)
↓
Consumer Service
↓
Console Output

---

## Key Concepts Learned

### @KafkaListener

`@KafkaListener` registers a method as a Kafka message listener.

Spring automatically:

- Creates a KafkaConsumer
- Subscribes to the topic
- Continuously polls Kafka
- Invokes the listener method for every received message

---

### Consumer Group

Consumers belonging to the same group share partitions.

Rules learned:

- One partition can only be consumed by one consumer within the same group.
- More consumers than partitions means some consumers remain idle.
- Kafka automatically rebalances consumers when group membership changes.

---

### Polling

Consumers do **not** receive pushed messages.

Instead they periodically execute:


poll()


to fetch new records.

---

### ConsumerRecord

Learned that Kafka messages contain metadata besides the message itself.

Useful fields:

- topic
- partition
- offset
- timestamp
- key
- value

---

### Offset

Every partition maintains its own offset sequence.

Example:

Partition 0

Offset 0
Offset 1
Offset 2
Offset 3

Offset identifies the exact position of a record inside its partition.

---

### Auto Commit

Default configuration:

enable-auto-commit=true

Kafka periodically commits offsets automatically.

Advantages:

- Easy configuration
- Minimal code

Disadvantages:

- Possible message loss if the application crashes after the offset is committed but before business logic finishes.

---

### Manual Commit

Configuration:

enable-auto-commit=false

listener:
  ack-mode: manual_immediate

Consumer explicitly decides when a message has been processed successfully.

Example:

acknowledgment.acknowledge();

Advantages:

- No message loss
- Better control
- Preferred for critical systems

Possible downside:

- Duplicate processing may occur if the application crashes before commit.

---

### Retry Mechanism

When an exception occurs before offset commit:

Business Logic
        ↓
Exception
        ↓
Offset NOT committed
        ↓
Spring Retry
        ↓
Kafka reads the same record again

Observed log:

Seeking to offset 4
Record in retry and not yet recovered

This demonstrates Spring Kafka's retry behavior.

---

## Delivery Semantics

We connected today's implementation with Kafka delivery guarantees.

| Strategy | Result |
|----------|--------|
| At Most Once | Possible message loss |
| At Least Once | Possible duplicate processing |
| Exactly Once | No loss and no duplicates (under specific conditions) |

For an event-driven e-commerce system, **At Least Once** is generally preferred because duplicate messages are easier to handle than lost orders.

---

## Test Result

PSuccessfully verified:

- Producer → Kafka → Consumer flow
- ConsumerRecord metadata
- Manual commit configuration
- Retry after exception
- Offset behavior

---

## Lessons Learned

- Kafka stores metadata together with every message.
- Offset belongs to a partition, not the entire topic.
- Commit timing determines delivery guarantees.
- Manual commit provides more reliability.
- Spring Kafka retries failed records automatically before giving up.

---

## Next Sprint

- ConsumerRecord
- Offset
- Partition
- Message Key
- Auto Commit
- Manual Commit
- Error Handling