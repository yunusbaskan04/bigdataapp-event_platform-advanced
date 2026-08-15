Sprint 08 - Kafka Connect & Debezium CDC
Goal

Implement a production-style Change Data Capture (CDC) pipeline that automatically publishes database events to Kafka using Kafka Connect and Debezium without writing Kafka producer code.

What We Learned
Kafka Connect

Kafka Connect is a framework for moving data between external systems and Kafka without writing custom integration code.

Instead of writing producers or consumers manually, Connect uses configurable connectors.

We learned:

Source Connector
Sink Connector
Distributed Workers
REST API
Connector Lifecycle
Source Connector vs Sink Connector
Database
    │
    ▼
Source Connector
    │
    ▼
Kafka
Kafka
   │
   ▼
Sink Connector
   │
   ▼
ElasticSearch
Debezium

Debezium is a Kafka Connect Source Connector that captures database changes.

Instead of querying the database repeatedly,

it reads

PostgreSQL WAL

and converts database changes into Kafka events.

Change Data Capture (CDC)

CDC means

Detect
Database Changes


↓


Publish Events

without modifying application code.

Instead of

KafkaTemplate.send(...)

events are generated automatically after a successful database commit.

PostgreSQL Logical Replication

Prepared PostgreSQL for CDC by enabling:

wal_level=logical
max_replication_slots
max_wal_senders

These settings allow PostgreSQL to expose committed changes through WAL.

Replication User

Created a dedicated Debezium user.

Granted

LOGIN
REPLICATION
SELECT

permissions.

Kafka Connect Container

Added Kafka Connect into Docker Compose.

Configured

Bootstrap Server
Plugin Path
Internal Topics

Verified

localhost:8083

REST API.

Debezium Connector

Created the first PostgreSQL connector.

Important configuration:

connector.class
topic.prefix
plugin.name
slot.name
publication.name
table.include.list
snapshot.mode
PostgreSQL Publication

Created

dbz_publication

to specify which tables Debezium should monitor.

Learned that Publication controls

Which tables are replicated

not the replication slot.

Replication Slot

Learned that PostgreSQL stores WAL until Debezium consumes it.

Observed

active = true

inside

pg_replication_slots

Also learned why an active replication slot cannot be dropped while Debezium is connected.

End-to-End CDC Flow

Verified the complete pipeline:

INSERT
    │
    ▼
Orders / Outbox Table
    │
    ▼
Database Commit
    │
    ▼
Write Ahead Log (WAL)
    │
    ▼
Debezium
    │
    ▼
Kafka Connect
    │
    ▼
Kafka Topic

No Kafka producer was involved.

Debezium Event Structure

Examined the generated CDC event.

Learned the purpose of

before
after
source
transaction
schema
payload
op
ts_ms
CRUD Operations

Observed operation types.

INSERT


op = c
UPDATE


op = u
DELETE


op = d
SNAPSHOT


op = r
Project Integration

Implemented the Transactional Outbox Pattern inside the Producer Service.

Instead of

kafkaTemplate.send(...)

the application now performs:

orderRepository.save(order);


outboxRepository.save(event);

inside a single database transaction.

Debezium automatically publishes the Outbox event to Kafka.

Problems Encountered & Solutions
Spring Boot Migration

While adding JPA and PostgreSQL dependencies, Maven dependencies were broken.

Resolved by restoring the correct Spring Boot parent configuration and refreshing Maven.

ObjectMapper Injection

IntelliJ could not detect an ObjectMapper bean.

Created

JacksonConfig

to explicitly register the bean.

JsonProcessingException

Discovered that newer Jackson versions throw

JacksonException

instead of JsonProcessingException in some APIs.

Adjusted the exception handling accordingly.

JSONB Mapping

Encountered

payload is of type jsonb

error while persisting Outbox events.

Solved by storing payload as TEXT (VARCHAR) because Debezium serializes it correctly into Kafka.

Debezium Verification

Verified

Connector RUNNING
Publication
Replication Slot
Kafka Topic creation
CDC Event generation

using

kafka-console-consumer
Key Concepts Learned
Kafka Connect Architecture
Source Connector
Debezium
Change Data Capture
PostgreSQL WAL
Logical Replication
Replication Slot
Publication
Kafka Connect REST API
Debezium Connector Configuration
CDC Event Structure
CRUD Event Types
Automatic Event Publishing
Transactional Outbox Implementation
Biggest Takeaway

For the first time in this project, Kafka events were produced without calling KafkaTemplate.send(). Events were generated automatically by monitoring PostgreSQL's Write-Ahead Log (WAL) through Debezium and Kafka Connect.

Next Sprint

Next we will simplify CDC events using Debezium Event Router (Outbox SMT).

Goals:

Event Router SMT
Clean Event Payloads
Remove Debezium Envelope
Publish business events directly
Prepare the architecture for multiple microservices