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
- [ ] Sprint 05 - PostgreSQL
- [ ] Sprint 06 - Kafka Connect
- [ ] Sprint 07 - Debezium CDC
- [ ] Sprint 08 - Kafka Streams
- [ ] Sprint 09 - Monitoring
- [ ] Sprint 10 - Production Improvements

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
@KafkaListener
│
▼
Consumer Service
│
ConsumerRecord
│
▼
Business Logic
│
Manual Commit
│
▼
Offset Commit


## Current Features

- Produce events using Spring Kafka
- Consume events with KafkaListener
- Consumer Groups
- ConsumerRecord metadata
- Manual Offset Commit
- Retry on failure