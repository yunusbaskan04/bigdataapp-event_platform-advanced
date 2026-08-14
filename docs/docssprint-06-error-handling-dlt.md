Sprint 06 - Consumer Error Handling & Dead Letter Topic
Goal

Learn how Spring Kafka handles failed messages, automatically retries processing, and safely isolates permanently failing records using a Dead Letter Topic (DLT).

What We Built
Configured DefaultErrorHandler.
Added retry support using FixedBackOff.
Simulated processing failures by throwing exceptions.
Observed Kafka retry behavior.
Created a Dead Letter Topic (orders-dlt).
Configured DeadLetterPublishingRecoverer.
Verified failed messages were published to the DLT after retries were exhausted.
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
Business Logic
        │
   Exception?
      │
 ┌────┴────┐
 │         │
No        Yes
 │         │
 ▼         ▼
Commit   Retry
            │
            ▼
     Retry Exhausted?
            │
     ┌──────┴──────┐
     │             │
    No            Yes
     │             │
     ▼             ▼
Retry Again     Publish to
                orders-dlt
Key Concepts Learned
DefaultErrorHandler

Spring Kafka provides a centralized error handler for consumer failures.

Configuration:

@Bean
public DefaultErrorHandler errorHandler() {


    FixedBackOff fixedBackOff =
            new FixedBackOff(1000L, 2);


    return new DefaultErrorHandler(fixedBackOff);
}

Responsibilities:

Handles listener exceptions.
Retries failed records.
Seeks back to the failed offset.
Delegates permanently failing records to a recoverer (optional).
FixedBackOff

Controls retry timing.

Example:

new FixedBackOff(
    1000L,
    2
);

Meaning:

Wait 1 second between retries.
Retry two additional times.

Observed execution:

1st attempt
↓


Retry #1
↓


Retry #2
↓


Retries exhausted
Retry Behavior

When the listener throws an exception before the offset is committed:

Consumer
    │
    ▼
Business Logic
    │
Exception
    │
    ▼
Offset NOT committed
    │
    ▼
Seek to previous offset
    │
    ▼
Retry

Observed logs:

Seeking to offset 12


Record in retry and not yet recovered

This shows Kafka reading the exact same record again.

Dead Letter Topic (DLT)

A Dead Letter Topic stores records that cannot be processed successfully after all retry attempts.

Configuration:

@Bean
public DefaultErrorHandler errorHandler(
        KafkaTemplate<Object, Object> kafkaTemplate) {


    DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(kafkaTemplate);


    FixedBackOff fixedBackOff =
            new FixedBackOff(1000L, 2);


    return new DefaultErrorHandler(
            recoverer,
            fixedBackOff
    );
}
DeadLetterPublishingRecoverer

Responsible for publishing failed records to another Kafka topic.

Default behavior:

orders
      │
Failure
      │
      ▼
orders-dlt

Partition mapping:

orders-0
↓


orders-dlt-0

The failed record keeps the same partition number whenever possible.

Retry + DLT Flow
Message arrives
        │
        ▼
Consumer
        │
Business Logic
        │
Exception
        ▼
Retry #1
        ▼
Retry #2
        ▼
Retries Exhausted
        ▼
DeadLetterPublishingRecoverer
        ▼
orders-dlt
Why DLT Exists

Without a DLT:

Bad Message
      │
Retry Forever
      │
Consumer Stuck

With a DLT:

Bad Message
      │
Retries
      │
DLT
      │
Consumer continues

One problematic record no longer blocks the entire consumer.

DLT Is Still Kafka

The Dead Letter Topic is simply another Kafka topic.

It:

stores records permanently (depending on retention),
can be consumed like any normal topic,
allows operators to inspect or replay failed events later.
Test Scenarios
Scenario 1

Listener throws an exception.

Result:

Retry executed.
Offset not committed.
Same record processed again.
Scenario 2

Retries exhausted.

Result:

Message published to orders-dlt.
Consumer continued processing new messages.
Scenario 3

Consume the DLT.

Command:

kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic orders-dlt \
--from-beginning

Result:

The failed event was successfully stored inside the Dead Letter Topic.

Lessons Learned
Spring Kafka retries failed records automatically.
FixedBackOff controls retry interval and retry count.
Kafka seeks back to the same offset during retries.
A failed record does not immediately disappear.
DeadLetterPublishingRecoverer publishes unrecoverable records to another topic.
Dead Letter Topics prevent a single bad message from blocking the consumer.
A DLT is simply another Kafka topic dedicated to failed events.
Next Sprint

In the next sprint we will explore Idempotent Processing and understand how applications safely handle duplicate message delivery in At-Least-Once systems.

Topics:

Why duplicate messages occur
Idempotency
Business keys
Database constraints
Safe retry strategies
Duplicate event handling