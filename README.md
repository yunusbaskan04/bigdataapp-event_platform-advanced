# Event Platform

A production-oriented Event-Driven E-Commerce platform built to learn Apache Kafka and modern data streaming technologies.

## Tech Stack

- Apache Kafka
- Spring Boot
- Java 21
- Docker & Docker Compose
- PostgreSQL
- Kafka Connect
- Debezium
- Kafka Streams

## Project Structure

event_platform/
├── producer-service/
├── consumer-service/
├── stream-service/
├── docs/
└── docker-compose.yml

## Current Progress

- [x] Sprint 01 - Repository Setup
- [x] Sprint 02 - Docker Compose & Kafka Bootstrap
- [x] Sprint 03 - Spring Boot Producer
- [x] Sprint 04 - Spring Boot Consumer
- [x] Sprint 05 - PostgreSQL
- [x] Sprint 06 - Reliable Consumer Processing
- [ ] Sprint 07 - Idempotency, Transactions & Outbox Pattern
- [ ] Sprint 08 - Kafka Connect
- [ ] Sprint 09 - Debezium CDC
- [ ] Sprint 10 - Kafka Streams
- [ ] Sprint 11 - Monitoring
- [ ] Sprint 12 - Production Improvements

## Current Architecture

Client
│
HTTP
│
▼
Producer Service
│
KafkaTemplate
│
▼
Kafka Topic (orders)
│
▼
Consumer Service
│
▼
Business Logic
│
├───────────────► Success
│                    │
│                    ▼
│             PostgreSQL
│                    │
│                    ▼
│             Manual Commit
│
└───────────────► Failure
                     │
                  Retry
                     │
          ┌──────────┴──────────┐
          │                     │
     Retry Success        Retry Exhausted
          │                     │
          ▼                     ▼
    Manual Commit         orders-dlt


## Current Features

## Current Features

- Produce events using Spring Kafka
- Consume events with KafkaListener
- Consumer Groups
- ConsumerRecord metadata
- JSON deserialization using Jackson
- Persist events into PostgreSQL
- Manual Offset Commit
- Retry on failure
- At-Least-Once processing
- Retry with DefaultErrorHandler
- FixedBackOff Retry Strategy
- Dead Letter Topic (DLT)
- DeadLetterPublishingRecoverer
- Consumer Lag Analysis
- Idempotent Consumer (Concept)
- Idempotent Producer (Concept)
- Kafka Transactions (Theory)
- Transactional Outbox Pattern (Theory)
- Change Data Capture (CDC)
- PostgreSQL WAL
