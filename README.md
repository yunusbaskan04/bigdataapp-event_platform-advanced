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
- [x] Sprint 07 - Idempotency, Transactions & Outbox Pattern
- [x] Sprint 08 - Kafka Connect & Debezium CDC
- [ ] Sprint 09 - Debezium Event Router (Outbox SMT)
- [ ] Sprint 10 - Kafka Streams
- [ ] Sprint 11 - Monitoring
- [ ] Sprint 12 - Production Improvements

## Current Architecture

                HTTP
                  │
                  ▼
         Producer Service
                  │
        Database Transaction
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
     Orders           Outbox Events
        │                   │
        └─────────┬─────────┘
                  ▼
          PostgreSQL WAL
                  │
                  ▼
             Debezium CDC
                  │
                  ▼
           Kafka Connect
                  │
                  ▼
 Kafka Topic (event-platform.public.outbox_events)
                  │
                  ▼
          Consumer Service


## Current Features

## Current Features

- Spring Boot Producer Service
- Spring Boot Consumer Service
- Apache Kafka Producer & Consumer
- PostgreSQL Integration
- Spring Data JPA
- Manual Offset Commit
- Retry Strategy
- Dead Letter Topic (DLT)
- At-Least-Once Delivery
- Consumer Lag Analysis
- Transactional Outbox Pattern
- Kafka Connect
- Debezium CDC
- PostgreSQL Logical Replication
- WAL (Write Ahead Log)
- Change Data Capture (CDC)
- Automatic Event Publishing