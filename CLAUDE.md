# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

An e-commerce order processing system built with 4 Spring Boot microservices communicating exclusively via Apache Kafka. Designed to demonstrate Kafka concepts in depth for technical interviews.

**Event flow:**
```
POST /api/orders → order-service → [orders.placed]
                                        ↓                  ↓
                               payment-service      inventory-service
                                        ↓                  ↓
                              [payments.processed]  [inventory.reserved]
                                        ↓     ↓           ↓     ↓
                               order-service   notification-service
                                 (updates)       (logs notifications)
```

## Build & Run Commands

The project includes a `mvnw` wrapper that downloads Maven automatically on first run. Maven 3.9.6 is downloaded to `~/.m2/wrapper/dists/`.

```bash
# Build all services (from root)
./mvnw clean package -DskipTests

# Run all tests across all services
./mvnw test

# Run tests for one service
./mvnw test -pl payment-service

# Run a single test class
./mvnw test -pl payment-service -Dtest=DlqIntegrationTest

# Run a single test method
./mvnw test -pl payment-service -Dtest=PaymentProcessingServiceTest#processPayment_shouldBeIdempotent

# Start everything with Docker
docker-compose up --build

# Start only infrastructure (Kafka + Zookeeper + Kafka UI)
docker-compose up zookeeper kafka kafka-ui

# Run a single service locally (after infrastructure is up)
./mvnw spring-boot:run -pl order-service
```

## Services

| Service | Port | Group ID | Produces | Consumes |
|---|---|---|---|---|
| order-service | 8081 | `order-service-group` | `orders.placed`, `orders.cancelled` | `payments.processed`, `inventory.reserved` |
| payment-service | 8082 | `payment-service-group` | `payments.processed`, `payments.dlq` | `orders.placed` |
| inventory-service | 8083 | `inventory-service-group` | `inventory.reserved` | `orders.placed`, `orders.cancelled` |
| notification-service | 8084 | `notification-service-group` | — | `payments.processed`, `inventory.reserved` |

Kafka UI: `http://localhost:8080`

## Kafka Topics

| Topic | Partitions | Key | Owner |
|---|---|---|---|
| `orders.placed` | 3 | orderId | order-service |
| `orders.cancelled` | 3 | orderId | order-service |
| `payments.processed` | 3 | orderId | payment-service |
| `payments.dlq` | 1 | orderId | payment-service |
| `inventory.reserved` | 3 | orderId | inventory-service |

Topics are created via `NewTopic` beans in each service's `KafkaTopicConfig.java`. `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` in Docker Compose.

## Package Structure (per service)

```
com.kafkademo.{service}/
├── config/
│   ├── KafkaProducerConfig.java   # ProducerFactory + KafkaTemplate beans
│   ├── KafkaConsumerConfig.java   # ConsumerFactory + ContainerFactory beans
│   ├── KafkaTopicConfig.java      # NewTopic beans
│   └── JacksonConfig.java         # ObjectMapper with JavaTimeModule
├── controller/
├── service/                       # Business logic, in-memory state
├── producer/                      # KafkaTemplate.send() wrappers
├── consumer/                      # @KafkaListener methods
└── model/                         # Events + domain objects
```

## Key Kafka Patterns in This Codebase

**Idempotent producer** (`KafkaProducerConfig.java` in order/payment/inventory):
- `ENABLE_IDEMPOTENCE_CONFIG=true`, `ACKS=all`, `RETRIES=3`, `MAX_IN_FLIGHT=1`

**Manual offset commit** (all consumer configs):
- `ENABLE_AUTO_COMMIT=false` + `AckMode.MANUAL_IMMEDIATE`
- `acknowledgment.acknowledge()` called only after successful processing

**Retry + DLQ** (`payment-service/config/KafkaConsumerConfig.java`):
- `DefaultErrorHandler` + `FixedBackOff(1000ms, 3 retries)`
- `DeadLetterPublishingRecoverer` routes failures to `payments.dlq`
- `PaymentProcessingException` is non-retryable — goes straight to DLQ

**Idempotent consumer** (`PaymentProcessingService`, `InventoryManagementService`):
- `ConcurrentHashMap.newKeySet()` tracks processed order IDs

**Consumer group isolation** (`payments.processed`):
- Both `order-service-group` and `notification-service-group` independently receive every message

**Consumer concurrency** (all services):
- `setConcurrency(3)` matches the 3 partitions — 1 thread per partition

**Batch consumption** (`notification-service`):
- `batchKafkaListenerContainerFactory` — `setBatchListener(true)` + `AckMode.BATCH` + `MAX_POLL_RECORDS=50`
- `BatchPaymentEventConsumer` receives `List<ConsumerRecord<String,String>>` and commits offsets once per batch
- Separate consumer group `notification-batch-group` so it tracks offsets independently

**Exactly-once semantics / EOS** (`order-service`):
- Separate `transactionalProducerFactory()` with `setTransactionIdPrefix("order-tx-")` — keeps existing producers non-transactional
- `KafkaTransactionManager` bean wired to the transactional factory
- `TransactionalOrderEventProducer` uses `kafkaTemplate.executeInTransaction()` — begins, sends, commits atomically
- Consumers with `isolation.level=read_committed` (Spring default) only see committed records

**Custom message headers** (`order-service` producer → `payment-service` consumer):
- `OrderEventProducer` wraps every send in a `ProducerRecord` with `RecordHeader` entries: `X-Correlation-Id`, `X-Event-Version`, `X-Source-Service`
- `payment-service/OrderEventConsumer` reads them via `@Header(value = "X-Correlation-Id", required = false)` — `required=false` ensures backward compatibility with older messages

## Testing

All integration tests use `@EmbeddedKafka` — no Docker required.

```java
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"orders.placed", "payments.processed"})
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
```

Test consumers read back messages using `KafkaTestUtils.consumerProps()` + `KafkaTestUtils.getRecords()`.

**DLQ test** (`DlqIntegrationTest`): call `POST /api/payments/force-fail/{orderId}` before publishing the order event, then wait ~10s for retry exhaustion.

## Demo Walkthrough

```bash
# 1. Create an order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"c1","productId":"prod-1","quantity":2,"totalAmount":99.99}'

# 2. Check order status (poll until CONFIRMED)
curl http://localhost:8081/api/orders/{orderId}

# 3. Force a DLQ scenario
curl -X POST http://localhost:8082/api/payments/force-fail/{orderId}
# Then create a new order with the same orderId flow

# 4. Check inventory
curl http://localhost:8083/api/inventory/prod-1

# 5. Check notifications
curl http://localhost:8084/api/notifications

# 6. Inspect DLQ in Kafka UI → Topics → payments.dlq
```

## Important Implementation Notes

- No Lombok — all POJOs use manual getters/setters/constructors
- Consumers deserialize raw JSON Strings (`StringDeserializer`) and parse with `ObjectMapper` — avoids cross-service class type header issues
- `JsonSerializer.ADD_TYPE_INFO_HEADERS=false` on producers for the same reason
- `@Lazy` on `KafkaTemplate` injection in `payment-service/KafkaConsumerConfig` breaks a circular bean dependency with `DeadLetterPublishingRecoverer`
- Initial inventory: `prod-1=100`, `prod-2=5` (low stock for INSUFFICIENT demo), `prod-3=50`
