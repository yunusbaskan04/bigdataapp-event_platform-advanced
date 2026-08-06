# Sprint 02 - Docker Compose & Kafka Bootstrap

## Objective

Bootstrap a single-node Apache Kafka cluster using Docker Compose and verify that Kafka can successfully produce and consume events.

---

## Completed Tasks

- Created the initial `docker-compose.yml`
- Learned the difference between Image, Container and Service
- Configured a single-node Kafka cluster using KRaft mode
- Configured Kafka listeners
- Configured controller quorum
- Configured advertised listeners
- Bootstrapped Kafka successfully
- Connected to the running container
- Explored Kafka image directory structure
- Located Kafka CLI tools
- Created the first Kafka topic
- Produced the first event
- Consumed the first event

---

## Docker Concepts Learned

### Image

A read-only template used to create containers.

Example:

```
apache/kafka:4.1.2
```

---

### Container

A running instance of an image.

---

### Service

A logical component defined inside Docker Compose.

One service may create one or multiple containers.

---

## Kafka Concepts Applied

### Node ID

Unique identifier of the Kafka node.

### Process Roles

Configured Kafka to work as:

- Broker
- Controller

inside the same process.

### Listeners

Kafka opens ports and waits for incoming connections.

```
PLAINTEXT://:9092
CONTROLLER://:9093
```

### Advertised Listeners

One of the most important Kafka configurations.

Difference learned:

Listeners

↓

Where Kafka actually listens.

Advertised Listeners

↓

Which address Kafka tells clients to use.

---

### Controller Quorum

Configured a single-node controller quorum.

```
1@kafka:9093
```

---

### Replication Factor

Current cluster has only one broker.

Therefore:

```
Replication Factor = 1
```

---

## Docker Networking

Learned the difference between

Host

```
localhost:19092
```

Container

```
localhost:9092
```

Docker Network

```
kafka:9092
```

---

## Linux Commands Learned

```
docker exec
find
ls -la
```

Explored Kafka installation under

```
/opt/kafka
```

Learned the purpose of

- bin
- config
- libs
- logs
- licenses

---

## Kafka CLI

Created topic

```
orders
```

Produced first event

Consumed first event

Verified Kafka cluster works correctly.

---

## Problems Encountered

### Advertised Listeners

Problem:

Kafka CLI inside the container received

```
localhost:19092
```

which is valid only for the host machine.

Solution:

Temporarily changed

```
localhost:19092
```

↓

```
localhost:9092
```

Future Sprint:

Implement INTERNAL / EXTERNAL listeners.

---

## Engineering Notes

Never copy Docker Compose blindly.

Every environment variable should answer one question:

- What does it do?
- Why is it needed?
- When would it change in production?

---

## Sprint Result

✔ Kafka Cluster Running

✔ First Topic Created

✔ First Event Produced

✔ First Event Consumed

Sprint Status

Completed