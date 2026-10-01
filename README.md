# Kafka Spring Boot — E-Commerce Event-Driven System

A production-style microservices system built to demonstrate Apache Kafka concepts in depth, designed for interview preparation.

## Architecture

```
POST /api/orders
      │
      ▼
┌─────────────┐   orders.placed    ┌─────────────────┐
│order-service│ ─────────────────► │ payment-service  │
│  (port 8081)│                    │   (port 8082)    │
│  order_db   │ ─────────────────► ├─────────────────┤
└─────────────┘   orders.placed    │inventory-service │
      ▲                            │   (port 8083)    │
      │                            │  inventory_db    │
      │  payments.processed        └─────────────────┘
      │◄──────────────────────────────────┤
      │  inventory.reserved               │
      │◄──────────────────────────────────┘
      │
      │  payments.processed + inventory.reserved
      ▼
┌──────────────────────┐     ┌─────────────────┐
│ notification-service │     │    payments.dlq  │
│     (port 8084)      │     │  (failed msgs)   │
│  notification_db     │     └─────────────────┘
└──────────────────────┘

Frontend Dashboard: http://localhost:3000
Kafka UI:           http://localhost:8080
```

### Service Summary

| Service | Port | DB | Produces | Consumes |
|---|---|---|---|---|
| order-service | 8081 | order_db | orders.placed, orders.cancelled | payments.processed, inventory.reserved |
| payment-service | 8082 | payment_db | payments.processed, payments.dlq | orders.placed |
| inventory-service | 8083 | inventory_db | inventory.reserved | orders.placed, orders.cancelled |
| notification-service | 8084 | notification_db | — | payments.processed, inventory.reserved |
| frontend | 3000 | — | — | REST APIs |

## Kafka Concepts Demonstrated

| Concept | Where in Code | Why It Matters |
|---|---|---|
| Idempotent Producer | `KafkaProducerConfig.java` in order/payment/inventory services | Prevents duplicate messages on retry; requires `ACKS=all` + `MAX_IN_FLIGHT=1` |
| Manual Offset Commit | All `*EventConsumer.java` classes | `acknowledgment.acknowledge()` called only after successful processing — prevents message loss |
| Consumer Groups | 4 distinct group IDs; `payments.processed` consumed by both `order-service-group` and `notification-service-group` | Two groups = two independent reads of every message; scales horizontally |
| Partition Key Strategy | orderId used as message key in all producers | All events for one order → same partition → guaranteed ordering |
| Concurrent Consumption | `setConcurrency(3)` in all `KafkaConsumerConfig.java` | 1 listener thread per partition; matches partition count of 3 |
| Retry + Dead Letter Queue | `payment-service/config/KafkaConsumerConfig.java` | `FixedBackOff(1000ms, 3)` + `DeadLetterPublishingRecoverer` → `payments.dlq` |
| Non-Retryable Exceptions | `KafkaConsumerConfig.java` in payment-service | `PaymentProcessingException` skips retry and goes straight to DLQ |
| Topic Creation via Beans | `KafkaTopicConfig.java` in each service | `NewTopic` beans + `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` = explicit ownership |
| JSON Serialization | `JsonSerializer` / `StringDeserializer` + `ObjectMapper` | `ADD_TYPE_INFO_HEADERS=false` prevents cross-service type binding issues |
| Idempotent Consumer | `PaymentProcessingService.existsByOrderId()`, `ReservationRepository.existsById()` | DB-backed deduplication survives service restarts (unlike in-memory Sets) |
| Offset Metadata Logging | Every `@KafkaListener` method | Logs `topic`, `partition`, `offset` for observability and debugging |

## Database Design

### order-service → order_db

Schema (`orders` table):

| Column | Type | Notes |
|---|---|---|
| order_id | VARCHAR(36) PK | UUID, used as Kafka message key |
| customer_id | VARCHAR(255) | |
| product_id | VARCHAR(255) | |
| quantity | INT | |
| total_amount | DECIMAL(19,2) | |
| status | VARCHAR(50) | Enum: PENDING → CONFIRMED / FAILED |
| payment_confirmed | BOOLEAN | Set to true when payment succeeds |
| stock_confirmed | BOOLEAN | Set to true when stock is reserved; both = CONFIRMED |
| placed_at | TIMESTAMP | |

**Replaced:** 3 `ConcurrentHashMap` fields. In production, in-memory state is lost on restart or when running multiple instances — the DB survives both.

### payment-service → payment_db

Schema (`payments` table):

| Column | Type | Notes |
|---|---|---|
| payment_id | VARCHAR(36) PK | UUID |
| order_id | VARCHAR(36) UNIQUE | Unique constraint enables idempotency check |
| status | VARCHAR(20) | SUCCESS or FAILED |
| failure_reason | TEXT | Null on success |
| processed_at | TIMESTAMP | |

**Replaced:** `payments` Map + `processedOrderIds` Set. The `UNIQUE` constraint on `order_id` means `existsByOrderId()` replaces the in-memory Set — and works correctly across multiple service instances.

### inventory-service → inventory_db

Schema (`stock_items` table):

| Column | Type | Notes |
|---|---|---|
| product_id | VARCHAR(255) PK | |
| available_stock | INT | CHECK >= 0 constraint prevents negative stock |

Schema (`reservations` table):

| Column | Type | Notes |
|---|---|---|
| order_id | VARCHAR(36) PK | Used as idempotency key |
| product_id | VARCHAR(255) | |
| quantity | INT | Restored on order cancellation |

**Replaced:** `stockLevels` Map, `reservations` Map, `orderProductMap`, `processedOrderIds` Set. The `@Transactional` reserve/release operations prevent race conditions that an in-memory `Map.compute()` cannot survive across distributed instances.

Seed data (from Flyway V1__init.sql):

| productId | initial stock |
|---|---|
| prod-1 | 100 |
| prod-2 | 5 (low — triggers INSUFFICIENT demo) |
| prod-3 | 50 |

### notification-service → notification_db

Schema (`notifications` table):

| Column | Type | Notes |
|---|---|---|
| id | VARCHAR(36) PK | UUID |
| order_id | VARCHAR(36) | Not a FK — services are decoupled |
| type | VARCHAR(100) | PAYMENT_SUCCESS, PAYMENT_FAILED, STOCK_RESERVED, STOCK_INSUFFICIENT |
| message | TEXT | Human-readable event description |
| sent_at | TIMESTAMP | |

**Replaced:** `Collections.synchronizedList`. The DB allows filtering by orderId and querying history — impossible with an in-memory list that resets on restart.

## Prerequisites

- Docker Desktop (for all services + Kafka + PostgreSQL)
- Java 17+ (for running tests or individual services locally)
- Nothing else — Maven is bundled via `./mvnw`

## Quick Start

```bash
# 1. Build all service JARs
./mvnw clean package -DskipTests

# 2. Start everything
docker-compose up --build

# 3. Wait ~30 seconds for all services to be healthy, then place an order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"alice","productId":"prod-1","quantity":2,"totalAmount":49.99}'
```

What to observe:

- **Dashboard**: http://localhost:3000 — watch the order flow through the architecture diagram in real time
- **Kafka UI**: http://localhost:8080 — inspect topics, consumer group lag, message payloads
- **Order status**: `curl http://localhost:8081/api/orders/{orderId}` — progresses from PENDING → CONFIRMED within ~5 seconds
- **Notifications**: `curl http://localhost:8084/api/notifications` — shows PAYMENT_SUCCESS + STOCK_RESERVED events
- **PostgreSQL**: `docker exec -it postgres psql -U postgres -d order_db -c "SELECT * FROM orders;"`

## Running Locally (without Docker)

Useful when iterating on a single service — start only the infrastructure in Docker, run services on the host.

### Step 1 — Start infrastructure only

```bash
docker-compose up zookeeper kafka kafka-ui postgres
```

This starts Kafka (port 9092), Zookeeper (2181), Kafka UI (8080), and PostgreSQL (5432). Wait ~15 seconds for Kafka to be ready.

### Step 2 — Build the JARs

```bash
./mvnw clean package -DskipTests
```

### Step 3 — Run each service

Open a separate terminal for each:

```bash
# Terminal 1
./mvnw spring-boot:run -pl order-service

# Terminal 2
./mvnw spring-boot:run -pl payment-service

# Terminal 3
./mvnw spring-boot:run -pl inventory-service

# Terminal 4
./mvnw spring-boot:run -pl notification-service
```

Each service's `application.yml` defaults to `localhost:9092` for Kafka and `localhost:5432` for PostgreSQL when `DB_HOST` and `SPRING_KAFKA_BOOTSTRAP_SERVERS` env vars are not set.

### Step 4 — Run the frontend

```bash
cd frontend
npm install
npm run dev   # → http://localhost:3000
```

The frontend defaults to `http://localhost:808{1-4}` for service URLs when the `*_SERVICE_URL` env vars are not set.

### Running a single service (fast iteration)

```bash
# Start infra + all other services via Docker, override just one locally:
docker-compose up zookeeper kafka kafka-ui postgres payment-service inventory-service notification-service

# Run only order-service on the host (picks up code changes on restart):
./mvnw spring-boot:run -pl order-service
```

## API Reference

### order-service (port 8081)

```bash
# Create order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"alice","productId":"prod-1","quantity":2,"totalAmount":49.99}'

# Get order status
curl http://localhost:8081/api/orders/{orderId}

# Cancel order
curl -X POST http://localhost:8081/api/orders/{orderId}/cancel
```

### payment-service (port 8082)

```bash
# Get payment by orderId
curl http://localhost:8082/api/payments/{orderId}

# Force a payment to fail and route to DLQ (used by frontend Force DLQ button)
curl -X POST http://localhost:8082/api/payments/force-fail/{orderId}
```

### inventory-service (port 8083)

```bash
# Check stock level
curl http://localhost:8083/api/inventory/prod-1

# Restock product
curl -X POST "http://localhost:8083/api/inventory/prod-2/restock?quantity=50"
```

### notification-service (port 8084)

```bash
# List all notifications
curl http://localhost:8084/api/notifications
```

## Running Tests

Tests use `@EmbeddedKafka` (no Docker) and H2 in-memory database (no PostgreSQL) — they run completely standalone.

```bash
# All tests across all services
./mvnw test

# One service only
./mvnw test -pl payment-service

# One test class
./mvnw test -pl payment-service -Dtest=DlqIntegrationTest

# One test method
./mvnw test -pl inventory-service -Dtest=InventoryManagementServiceTest#reserveStock_shouldBeIdempotent
```

What each test proves:

| Test | Type | Proves |
|---|---|---|
| `OrderEventProducerTest` | @EmbeddedKafka | Producer writes correct key+value to `orders.placed` |
| `PaymentEventConsumerTest` (order-service) | @EmbeddedKafka | Order status updates when payment event arrives |
| `OrderServiceTest` | Unit (Mockito) | Business logic: PENDING → CONFIRMED flow, idempotency |
| `OrderEventConsumerTest` (payment-service) | @EmbeddedKafka | End-to-end: receive order → publish payment result |
| `DlqIntegrationTest` | @EmbeddedKafka | Failed processing → `payments.dlq` after retries exhausted |
| `PaymentProcessingServiceTest` | Unit (Mockito) | DB idempotency, force-fail, success path |
| `OrderEventConsumerTest` (inventory-service) | @EmbeddedKafka | Reservation flow: RESERVED and INSUFFICIENT scenarios |
| `PaymentEventConsumerTest` (notification-service) | @EmbeddedKafka | PAYMENT_SUCCESS/FAILED triggers correct notification type |

## Interview Prep Cheat Sheet

> **Q: What is a Kafka consumer group and why does it matter?**
>
> **A:** A consumer group is a set of consumers that collectively read from a topic. Each partition is assigned to exactly one consumer in the group at a time. In this project, `payments.processed` has two groups — `order-service-group` and `notification-service-group` — so both services independently receive every message. See: any `groupId` in `KafkaConsumerConfig.java`.

---

> **Q: How does this project prevent duplicate message processing?**
>
> **A:** Two layers. Producer-side: `ENABLE_IDEMPOTENCE_CONFIG=true` prevents duplicates on retry. Consumer-side: `PaymentProcessingService` checks `paymentRepository.existsByOrderId()` before processing — a DB-backed check that survives restarts. See: `KafkaProducerConfig.java`, `PaymentProcessingService.java`.

---

> **Q: What is a Dead Letter Queue and when does a message end up there?**
>
> **A:** A DLQ is a topic that receives messages that failed processing after all retries. In `payment-service`, `DefaultErrorHandler` with `FixedBackOff(1000ms, 3)` retries 3 times. If the exception is non-retryable (`PaymentProcessingException`), it goes to `payments.dlq` immediately. See: `KafkaConsumerConfig.java` in payment-service.

---

> **Q: Why use orderId as the Kafka message key?**
>
> **A:** The key determines which partition a message goes to. Using orderId as the key guarantees all events for the same order (placed, payment, stock) go to the same partition and are processed in order. Without this, a payment event could be processed before the order event on a different partition.

---

> **Q: What is manual offset commit and why is it important?**
>
> **A:** By default, Kafka auto-commits offsets on a timer, meaning if a consumer crashes mid-processing, the offset is already committed and the message is lost. With `ENABLE_AUTO_COMMIT=false` and `acknowledgment.acknowledge()` called only after successful processing, a crash before ack causes the message to be re-delivered. See: all `@KafkaListener` methods.

---

> **Q: What does `setConcurrency(3)` do?**
>
> **A:** Creates 3 listener threads, one per partition. Since all topics have 3 partitions, each thread is exclusively responsible for one partition. This is the maximum useful concurrency — adding more threads than partitions leaves some idle. See: `KafkaConsumerConfig.java`, `setConcurrency(3)`.

---

> **Q: What is the difference between `auto.offset.reset=earliest` vs `latest`?**
>
> **A:** `earliest` starts reading from the beginning of the topic if no committed offset exists (e.g., new consumer group). `latest` only reads new messages. All services use `earliest` so they don't miss events published before they started. See: `application.yml` in each service.

---

> **Q: What does `ACKS=all` mean and why is it paired with idempotence?**
>
> **A:** `ACKS=all` requires acknowledgment from all in-sync replicas before considering a message produced — the strongest durability guarantee. It's paired with `ENABLE_IDEMPOTENCE=true` because retries (needed for `ACKS=all`) could create duplicates without idempotence. `MAX_IN_FLIGHT=1` ensures ordering when retrying.

---

> **Q: Why is `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` set in Docker Compose?**
>
> **A:** Forcing explicit topic creation via `NewTopic` beans ensures topics are created with the right partition count and replication factor. Auto-creation silently creates topics with defaults (often 1 partition), hiding configuration drift.

---

> **Q: How does Flyway integrate with Spring Boot?**
>
> **A:** Spring Boot auto-detects Flyway when `flyway-core` is on the classpath. On startup, Flyway checks `flyway_schema_history` for applied migrations. New `V{n}__description.sql` files in `classpath:db/migration` are applied in version order. `ddl-auto: validate` makes Hibernate validate that entity definitions match the schema Flyway created — catching drift at startup.

---

> **Q: Why does each microservice have its own database?**
>
> **A:** Database-per-service prevents tight coupling. If services share a DB, a schema change for one service can break another. It also allows each service to choose the right storage technology independently. The `UNIQUE` constraint on `payments.order_id` is a payment-service concern — order-service has no visibility into it.

---

> **Q: How would you scale this system for higher throughput?**
>
> **A:** Increase partition count (e.g., from 3 to 9). Run multiple instances of each service — each instance handles a subset of partitions automatically via consumer group rebalancing. The DB-backed idempotency checks (`existsByOrderId`, `existsById`) are critical here: without them, two instances could process the same message simultaneously.

## Project Structure

| Directory | Description |
|---|---|
| `order-service/` | REST entry point, order lifecycle management |
| `payment-service/` | Payment processing with DLQ and retry |
| `inventory-service/` | Stock management with atomic reservation |
| `notification-service/` | Event log and notification fanout |
| `frontend/` | Next.js dashboard (React Flow + live event log) |
| `postgres/` | Database initialization script |
| `docs/` | Design specs and architecture documentation |
