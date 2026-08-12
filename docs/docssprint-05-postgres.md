Sprint 05 - Consumer Reliability & Offset Management
Goal

Understand how Kafka determines whether a message has been successfully processed and learn how offset commits affect message delivery guarantees.

What We Built
Connected the Kafka Consumer to PostgreSQL.
Persisted incoming events into the database.
Disabled automatic offset commits.
Implemented manual acknowledgment using Acknowledgment.
Experimented with different commit scenarios.
Observed Kafka's behavior after consumer restarts.
Learned how Kafka achieves At-Least-Once Delivery.
Architecture
Producer Service
        │
        ▼
Kafka Topic (orders)
        │
        ▼
Kafka Consumer
        │
        ▼
Deserialize JSON
        │
        ▼
Business Logic
        │
        ▼
PostgreSQL
        │
        ▼
acknowledgment.acknowledge()
        │
        ▼
Offset Commit
Key Concepts Learned
Automatic Offset Commit

Configuration:

enable-auto-commit: true

Kafka periodically commits offsets in the background.

Advantages

Very easy to use
Minimal configuration

Disadvantages

Offsets may be committed before business logic finishes.
If the application crashes afterwards, Kafka assumes the message was processed successfully.
This may lead to message loss.
Manual Offset Commit

Configuration

consumer:
  enable-auto-commit: false

listener:
  ack-mode: manual_immediate

Consumer decides exactly when the offset should be committed.

Example

orderService.save(order);

acknowledgment.acknowledge();

Advantages

Business logic finishes before committing.
Higher reliability.
Preferred in production systems.

Possible drawback

If the application crashes before acknowledgment, Kafka delivers the same message again.
Acknowledgment

Calling

acknowledgment.acknowledge();

does not save data into PostgreSQL.

Instead, it tells Kafka:

"I have successfully processed this record. You may commit its offset."

Without acknowledgment:

Database transaction may succeed.
Kafka still considers the record unprocessed.
The same record will be delivered again.
Database Commit vs Offset Commit

One of the most important lessons learned.

These are two completely independent operations.

Kafka Message
      │
      ▼
Business Logic
      │
      ├────────► PostgreSQL Commit
      │
      ▼
Kafka Offset Commit

Writing data into PostgreSQL does not automatically commit the Kafka offset.

Likewise, committing the Kafka offset does not affect the database.

Consumer Restart Experiment

Experiment:

Disable acknowledgment.
Save data into PostgreSQL.
Stop the consumer.
Start the consumer again.

Observed result:

Kafka delivered the same message again.
Because the offset had never been committed.

This confirmed that Kafka tracks offsets, not database state.

Consumer Offline Experiment

Experiment:

Stop the consumer.
Send new events.
Start the consumer again.

Observed result:

No data was lost.
Kafka stored all events while the consumer was offline.
Consumer processed every message after reconnecting.

This demonstrates Kafka's durability.

Idempotent Processing

During testing the same message was processed multiple times.

However, PostgreSQL contained only one row.

Reason:

orderId

is the primary key.

When Kafka redelivered the event, the database state remained consistent.

This is an example of idempotent processing, a common technique for handling duplicate deliveries in At-Least-Once systems.

Delivery Semantics
Strategy	Description
At Most Once	No duplicates, but messages may be lost.
At Least Once	Messages are never lost, but duplicates are possible.
Exactly Once	No duplicates and no message loss (under specific conditions).

Current implementation:

At-Least-Once Delivery
Test Scenarios
Scenario 1
save()

acknowledge()

Result

Database updated.
Offset committed.
Message never delivered again.
Scenario 2
save()

// acknowledge()

Result

Database updated.
Offset not committed.
Message delivered again after consumer restart.
Scenario 3

Consumer offline

↓

Producer sends events

↓

Consumer starts

↓

Kafka delivers all stored events

Result

No message loss.
Lessons Learned
Kafka tracks processing using offsets.
Offset commit and database commit are independent operations.
Manual acknowledgment gives the application full control over commits.
Kafka does not know whether business logic succeeded.
Kafka only knows whether an offset has been committed.
At-Least-Once delivery is achieved through manual offset management.
Duplicate processing should be expected and handled by the application.
Next Sprint

In the next sprint we will move inside Kafka itself and explore how offsets are stored internally.

Topics:

__consumer_offsets
Offset Commit internals
Consumer Group Coordinator
Offset Recovery
Consumer Restart
Partition Assignment
Rebalancing