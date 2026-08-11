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

## Test Result

Producer successfully published messages.

Consumer successfully received and processed them.


Received : First Order


---

## Next Sprint

- ConsumerRecord
- Offset
- Partition
- Message Key
- Auto Commit
- Manual Commit
- Error Handling