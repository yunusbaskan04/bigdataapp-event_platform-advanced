Sprint 09 - Transactional Outbox Event Router (SMT)
Goal

Transform raw Debezium CDC events into clean business events using Debezium Outbox Event Router (Single Message Transform).

Instead of publishing verbose CDC envelopes, publish domain events directly to Kafka topics.

What We Learned
Why Event Router (SMT)?

Raw Debezium events contain a large amount of metadata.

Example:

before
after
source
transaction
op
ts_ms
schema

Business services usually do not need this information.

Instead they only care about the business payload.

Without SMT

Application


↓


Outbox Table


↓


Debezium CDC Event


↓


Kafka

With SMT

Application


↓


Outbox Table


↓


Debezium Event Router


↓


Clean Business Event


↓


Kafka

The Event Router removes the Debezium envelope and publishes only the business event.

Transactional Outbox Table Design

Extended the Outbox table with metadata required by the Event Router.

Added fields:

aggregate_type
aggregate_id
event_type
payload
created_at

Purpose of each field:

Field	Purpose
aggregate_type	Determines the destination Kafka topic
aggregate_id	Used as the Kafka message key
event_type	Represents the business event type
payload	Business event content
created_at	Event creation timestamp
Debezium Event Router Configuration

Configured the connector with the Outbox SMT.

Important properties:

transforms=outbox


transforms.outbox.type=io.debezium.transforms.outbox.EventRouter


transforms.outbox.route.by.field=aggregate_type


transforms.outbox.route.topic.replacement=${routedByValue}


transforms.outbox.table.field.event.key=aggregate_id


transforms.outbox.table.field.event.type=event_type

Learned how each property affects event routing.

Topic Routing Strategy

Instead of sending every event into a single topic,

events are routed dynamically according to

aggregate_type

Example

aggregate_type = orders


↓


orders Topic

Future examples

payments


↓


payments Topic
users


↓


users Topic

This allows every bounded context to own its own Kafka topic.

Kafka Message Key

Configured

aggregate_id

as the Kafka message key.

Benefits:

Stable partitioning
Ordering guarantee per aggregate
Related events always go to the same partition
Clean Business Events

Before SMT

{
  before: ...
  after: ...
  source: ...
  op: "c"
  ts_ms: ...
}

After SMT

{
  "orderId":600,
  "product":"Gaming Chair"
}

Consumers now receive only the business data they need.

Connector Management

Created helper scripts for connector management.

register-outbox-connector.bat

Registers the connector.

delete-outbox-connector.bat

Deletes the connector before applying configuration changes.

This made connector updates much faster during development.

Problems Encountered & Solutions
Debezium Event Router Failed

Error

aggregateid is not a valid field name

Cause

Debezium expects database column names such as

aggregate_id

not Java entity field names like

aggregateId

Solution

Configured the connector to explicitly map the database columns.

transforms.outbox.table.field.event.key=aggregate_id


transforms.outbox.table.field.event.type=event_type
Wrong JPA Column Mapping

Initially the entity used

@Column(name="aggregateid")

instead of

@Column(name="aggregate_id")

Debezium reads PostgreSQL column names, not Java property names.

Fixed by matching the entity with the database schema.

Messages Published to Wrong Topic

Initially events appeared in

outbox.event.orders

instead of

orders

Reason

Debezium Event Router uses

outbox.event.${routedByValue}

as the default topic naming convention.

Solution

Configured

transforms.outbox.route.topic.replacement=${routedByValue}

Now events are published directly to

orders
Connector Entered FAILED State

Learned that Kafka Connect does not automatically recover from SMT configuration errors.

After fixing the configuration:

Delete Connector
Register Connector again
Verify
RUNNING

status.

Verification

Successfully verified the complete production flow.

POST /orders


↓


Order Entity


↓


Outbox Event


↓


Database Commit


↓


PostgreSQL WAL


↓


Debezium


↓


Event Router SMT


↓


orders Topic


↓


Kafka Consumer

The consumer now receives only the business payload without any Debezium metadata.

Key Concepts Learned
Debezium Single Message Transform (SMT)
Event Router
Transactional Outbox Metadata
Dynamic Topic Routing
Kafka Message Keys
Aggregate-Based Routing
Clean Business Events
Connector Lifecycle
Connector Registration
Connector Reconfiguration
Production CDC Architecture
Biggest Takeaway

The application never publishes directly to Kafka.

The Producer Service only writes data into the Order and Outbox tables inside the same database transaction.

Debezium monitors PostgreSQL's WAL, Event Router transforms the CDC record into a clean business event, and Kafka receives the final message automatically.

This is the architecture commonly used in production microservice systems because it guarantees consistency while keeping business services completely independent from Kafka.

Next Sprint

Next we will explore Kafka Streams and learn how to process events directly inside Kafka.

Goals:

- Kafka Streams Architecture
- KStream
- KTable
- Stateless Operations
- Stateful Operations
- Filtering & Mapping
- Aggregations
- Windowing
- Stream Topologies