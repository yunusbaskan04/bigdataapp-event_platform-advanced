# Sprint 03 - Spring Boot Kafka Producer

## Objective

Create a Spring Boot Producer service capable of publishing events to Apache Kafka.

---

## Completed Tasks

- Created Producer Service using Spring Boot
- Added Spring for Apache Kafka dependency
- Configured Kafka bootstrap server
- Learned Spring Boot Kafka Auto Configuration
- Injected KafkaTemplate using Dependency Injection
- Implemented OrderProducer component
- Implemented REST endpoint for sending messages
- Produced events from HTTP requests
- Verified messages using Kafka Console Consumer
- Investigated Kafka Docker image networking issue
- Replaced the Kafka Docker image with `apache/kafka-native`
- Verified end-to-end message delivery

---

## Lessons Learned

### KafkaTemplate

`KafkaTemplate` is Spring Kafka's high-level API for publishing messages to Kafka.

Instead of using the native Kafka Producer API directly, Spring provides KafkaTemplate to simplify producing events.

---

### Spring Boot Auto Configuration

No `@Bean` configuration was required.

Spring Boot automatically created:

- ProducerFactory
- KafkaTemplate
- KafkaProducer

using the values defined in `application.yml`.

---

### Producer Flow


HTTP Request

↓

OrderController

↓

OrderProducer

↓

KafkaTemplate

↓

Kafka Broker

↓

orders Topic

↓

Consumer


---

### Kafka Networking Issue

Initially the project used the `apache/kafka` Docker image.

Although Spring Boot successfully produced messages, Kafka CLI tools inside the container could not communicate correctly because of networking behavior in that image.

Switching to `apache/kafka-native` resolved the issue, and both Spring Boot and Kafka CLI tools worked correctly.

---

## Outcome

Successfully published Kafka events from a Spring Boot application and verified them using Kafka Console Consumer.