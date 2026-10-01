# Design: PostgreSQL Integration + README

**Date:** 2026-04-04
**Project:** kafka-springboot-project
**Scope:** Add PostgreSQL persistence to all 4 services using Spring Data JPA + Flyway; write comprehensive README.md.

---

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| DB topology | One PostgreSQL container, four separate databases | Database-per-service microservices pattern; correct for interviews |
| Schema migration | Flyway versioned migrations | Real-world practice; teaches V1__init.sql pattern; auto-applied on startup |
| Entity strategy | Annotate existing model classes | No redundant mapping layer; idiomatic Spring Boot |
| README style | Full design doc + setup guide | Project exists for learning; "why" matters as much as "how" |

---

## Section 1: Infrastructure

### New Files
- `postgres/init.sql` — creates four databases on first container start
- Modified: `docker-compose.yml`

### PostgreSQL Container
```yaml
postgres:
  image: postgres:15
  environment:
    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: postgres
    POSTGRES_DB: postgres        # default db; init.sql creates the four service DBs
  ports:
    - "5432:5432"
  volumes:
    - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    - postgres_data:/var/lib/postgresql/data
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U postgres"]
    interval: 10s
    timeout: 5s
    retries: 5
```

### Service Dependencies
Each service gains:
```yaml
depends_on:
  postgres:
    condition: service_healthy
  kafka:
    condition: service_started
environment:
  SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  DB_HOST: postgres
```

### `postgres/init.sql`
```sql
CREATE DATABASE order_db;
CREATE DATABASE payment_db;
CREATE DATABASE inventory_db;
CREATE DATABASE notification_db;
```

---

## Section 2: Database Schema + Flyway Migrations

Each service gets `src/main/resources/db/migration/V1__init.sql`.
Flyway is auto-configured by Spring Boot when `spring-boot-starter-data-jpa` and `flyway-core` are on the classpath.

### order_db — `orders` table
Replaces: `orderStore` (ConcurrentHashMap), `paymentConfirmedMap`, `stockConfirmedMap`

```sql
CREATE TABLE orders (
  order_id          VARCHAR(36)    PRIMARY KEY,
  customer_id       VARCHAR(255)   NOT NULL,
  product_id        VARCHAR(255)   NOT NULL,
  quantity          INT            NOT NULL,
  total_amount      DECIMAL(19,2)  NOT NULL,
  status            VARCHAR(50)    NOT NULL,
  payment_confirmed BOOLEAN        NOT NULL DEFAULT FALSE,
  stock_confirmed   BOOLEAN        NOT NULL DEFAULT FALSE,
  placed_at         TIMESTAMP      NOT NULL
);
```

### payment_db — `payments` table
Replaces: `payments` map, `processedOrderIds` set
Idempotency: `existsByOrderId(orderId)` — if a row exists, skip processing.

```sql
CREATE TABLE payments (
  payment_id    VARCHAR(36)  PRIMARY KEY,
  order_id      VARCHAR(36)  UNIQUE NOT NULL,
  status        VARCHAR(20)  NOT NULL,
  failure_reason TEXT,
  processed_at  TIMESTAMP    NOT NULL
);
```

### inventory_db — `stock_items` + `reservations` tables
Replaces: `stockLevels` map, `reservations` map, `orderProductMap`, `processedOrderIds` set
Idempotency: `reservationRepository.existsById(orderId)` — if a reservation row exists, skip.
Atomicity: `@Transactional` on reserve/release methods.

```sql
CREATE TABLE stock_items (
  product_id      VARCHAR(255) PRIMARY KEY,
  available_stock INT          NOT NULL CHECK (available_stock >= 0)
);

CREATE TABLE reservations (
  order_id   VARCHAR(36)  PRIMARY KEY,
  product_id VARCHAR(255) NOT NULL,
  quantity   INT          NOT NULL
);

-- Seed data (replaces @PostConstruct initialization)
INSERT INTO stock_items (product_id, available_stock) VALUES
  ('prod-1', 100),
  ('prod-2', 5),
  ('prod-3', 50);
```

### notification_db — `notifications` table
Replaces: `List<Notification>` (synchronized list)

```sql
CREATE TABLE notifications (
  id       VARCHAR(36)  PRIMARY KEY,
  order_id VARCHAR(36)  NOT NULL,
  type     VARCHAR(100) NOT NULL,
  message  TEXT         NOT NULL,
  sent_at  TIMESTAMP    NOT NULL
);
```

---

## Section 3: JPA Entities, Repositories, and Service Changes

### Dependencies Added to Each Service POM
```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>postgresql</artifactId>
  <scope>runtime</scope>
</dependency>
<dependency>
  <groupId>org.flywaydb</groupId>
  <artifactId>flyway-core</artifactId>
</dependency>
<!-- Test scope: H2 so @SpringBootTest + @EmbeddedKafka tests run without a live Postgres -->
<dependency>
  <groupId>com.h2database</groupId>
  <artifactId>h2</artifactId>
  <scope>test</scope>
</dependency>
```

### Test Datasource Strategy
The existing `@SpringBootTest` + `@EmbeddedKafka` tests must not require a running PostgreSQL. Each service gets `src/test/resources/application-test.yml` that overrides the datasource with H2 in-memory and sets Flyway to use its H2-compatible dialect:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: create-drop     # H2 test profile: let Hibernate manage schema
  flyway:
    enabled: false              # Flyway disabled in tests; Hibernate creates schema from entities
```

Tests are activated via `@ActiveProfiles("test")` on each `@SpringBootTest` class, or by adding `spring.profiles.active=test` to `@TestPropertySource`. The `application-test.yml` file takes precedence over `application.yml` for any matching keys.

### `application.yml` additions (per service)
```yaml
spring:
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:5432/{service}_db
    username: postgres
    password: postgres
    driver-class-name: org.postgresql.Driver
  jpa:
    hibernate:
      ddl-auto: validate          # Flyway owns schema; Hibernate only validates
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        format_sql: true
  flyway:
    enabled: true
    locations: classpath:db/migration
```

`ddl-auto: validate` is intentional — Flyway creates the schema, Hibernate validates it matches the entities. This catches drift between migrations and entity definitions.

### Entity Annotations (applied to existing model classes)

**Order.java**
```java
@Entity
@Table(name = "orders")
public class Order {
    @Id
    private String orderId;
    // ... existing fields unchanged ...
    @Enumerated(EnumType.STRING)
    private OrderStatus status;
    private boolean paymentConfirmed;   // new field (replaces paymentConfirmedMap)
    private boolean stockConfirmed;     // new field (replaces stockConfirmedMap)
}
```

**Payment.java** — add `@Entity @Table(name = "payments")`, `@Id` on `paymentId`, `@Enumerated` on `status`.

**StockItem.java** — add `@Entity @Table(name = "stock_items")`, `@Id` on `productId`.

**Notification.java** — add `@Entity @Table(name = "notifications")`, `@Id` on `id`.

New entity: **Reservation.java** (inventory-service only)
```java
@Entity
@Table(name = "reservations")
public class Reservation {
    @Id
    private String orderId;
    private String productId;
    private int quantity;
}
```

### New Repository Interfaces

| Service | Interface | Key methods |
|---|---|---|
| order-service | `OrderRepository extends JpaRepository<Order, String>` | `findById`, `save` |
| payment-service | `PaymentRepository extends JpaRepository<Payment, String>` | `existsByOrderId`, `save` |
| inventory-service | `StockItemRepository extends JpaRepository<StockItem, String>` | `findById`, `save` |
| inventory-service | `ReservationRepository extends JpaRepository<Reservation, String>` | `existsById`, `save`, `findById` |
| notification-service | `NotificationRepository extends JpaRepository<Notification, String>` | `save`, `findAll` |

### Service Layer Changes

**OrderService** — inject `OrderRepository`; remove 3 `ConcurrentHashMap` fields:
- `createOrder` → `orderRepository.save(order)`
- `getOrder` → `orderRepository.findById(orderId)`
- `cancelOrder` → `orderRepository.findById` + update status + `save`
- `handlePaymentProcessed` / `handleStockReserved` → load order, set `paymentConfirmed`/`stockConfirmed` on entity, check both booleans for `CONFIRMED`, save

**PaymentProcessingService** — inject `PaymentRepository`; remove `processedOrderIds` set and `payments` map:
- Idempotency check: `if (paymentRepository.existsByOrderId(orderId)) return;`
- Save on both success and failure paths

**InventoryManagementService** — inject `StockItemRepository` + `ReservationRepository`; remove 4 in-memory fields; `@Transactional` on `reserveStock` and `releaseStock`:
- Idempotency: `if (reservationRepository.existsById(orderId)) return;`
- `reserveStock` → `stockItemRepository.findById` → compute new stock → `stockItemRepository.save` + `reservationRepository.save`
- `releaseStock` → `reservationRepository.findById` → restore stock → delete reservation
- Remove `@PostConstruct` (seed data is in Flyway `V1__init.sql`)

**NotificationService** — inject `NotificationRepository`; remove `List<Notification>`:
- `sendPaymentNotification` / `sendStockNotification` → `notificationRepository.save(notification)`
- `getRecentNotifications` → `notificationRepository.findAll()`

---

## Section 4: README.md Structure

File: `/README.md`

1. **Project Overview** — what it is, who it's for, one-sentence per service
2. **Architecture** — ASCII event flow + service/port/DB/topic table
3. **Kafka Concepts Demonstrated** — table: concept → file → explanation
4. **Database Design** — per service: schema table + what it replaces + why it matters
5. **Prerequisites** — Docker Desktop, Java 17+
6. **Quick Start** — 3 commands to be running, first curl, what to observe
7. **API Reference** — all endpoints with curl examples
8. **Running Tests** — `./mvnw test`, single service, single class, single method
9. **Interview Prep Cheat Sheet** — Q&A: 10 Kafka + 5 DB questions, each with a code pointer

---

## Files Created / Modified

### New Files
```
postgres/init.sql
order-service/src/test/resources/application-test.yml
payment-service/src/test/resources/application-test.yml
inventory-service/src/test/resources/application-test.yml
notification-service/src/test/resources/application-test.yml
order-service/src/main/resources/db/migration/V1__init.sql
payment-service/src/main/resources/db/migration/V1__init.sql
inventory-service/src/main/resources/db/migration/V1__init.sql
  (includes seed INSERT statements)
notification-service/src/main/resources/db/migration/V1__init.sql
inventory-service/src/main/java/com/kafkademo/inventory/model/Reservation.java
inventory-service/src/main/java/com/kafkademo/inventory/repository/ReservationRepository.java
order-service/src/main/java/com/kafkademo/order/repository/OrderRepository.java
payment-service/src/main/java/com/kafkademo/payment/repository/PaymentRepository.java
inventory-service/src/main/java/com/kafkademo/inventory/repository/StockItemRepository.java
notification-service/src/main/java/com/kafkademo/notification/repository/NotificationRepository.java
README.md
```

### Modified Files
```
docker-compose.yml
order-service/pom.xml
payment-service/pom.xml
inventory-service/pom.xml
notification-service/pom.xml
order-service/src/main/resources/application.yml
payment-service/src/main/resources/application.yml
inventory-service/src/main/resources/application.yml
notification-service/src/main/resources/application.yml
order-service/src/main/java/com/kafkademo/order/model/Order.java
payment-service/src/main/java/com/kafkademo/payment/model/Payment.java
inventory-service/src/main/java/com/kafkademo/inventory/model/StockItem.java
notification-service/src/main/java/com/kafkademo/notification/model/Notification.java
order-service/src/main/java/com/kafkademo/order/service/OrderService.java
payment-service/src/main/java/com/kafkademo/payment/service/PaymentProcessingService.java
inventory-service/src/main/java/com/kafkademo/inventory/service/InventoryManagementService.java
notification-service/src/main/java/com/kafkademo/notification/service/NotificationService.java
CLAUDE.md
```

---

## Verification

1. `./mvnw clean package -DskipTests` — all 4 services build
2. `docker-compose up --build` — all containers healthy (postgres healthcheck passes before services start)
3. Connect to postgres: `docker exec -it <postgres-container> psql -U postgres -d order_db -c "\dt"` — verify `orders` table exists + `flyway_schema_history` row
4. Place an order: `curl -X POST http://localhost:8081/api/orders ...`
5. Query order DB: `psql -d order_db -c "SELECT * FROM orders;"` — row exists with `CONFIRMED` status
6. Query payment DB, inventory DB, notification DB similarly
7. `./mvnw test` — all tests pass (unit tests unaffected; integration tests need embedded DB or `@DataJpaTest`)
