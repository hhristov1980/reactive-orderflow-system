# OrderFlow

[![CI/CD](https://github.com/hhristov1980/reactive-orderflow-system/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/hhristov1980/reactive-orderflow-system/actions/workflows/ci-cd.yml)

OrderFlow is a reactive, event-driven order management platform built with Spring WebFlux, R2DBC, PostgreSQL, Kafka, Docker, MapStruct, Prometheus, Grafana, and GitHub Actions.

It demonstrates a realistic e-commerce backend flow: product catalog management, users, order creation, inventory reservation, payment processing, shipping, audit logging, scheduled unpaid payment expiration, reporting, observability, and operational admin views.

The project is currently implemented as a modular monolith with clear bounded contexts, making it suitable for later extraction into separate microservices.

---

## Main Goals

The project is designed to demonstrate:

* Reactive programming with Spring WebFlux and Project Reactor
* Non-blocking database access with R2DBC
* Event-driven communication with Apache Kafka
* Reliable event publishing through the Transactional Outbox Pattern
* Kafka consumer retries with dead-letter topics for failed records
* Saga-like order lifecycle coordination
* Concurrent-safe inventory updates with atomic SQL
* Idempotent inventory reservation and release handling for duplicate Kafka delivery
* Reactive transactional operations with `TransactionalOperator`
* Parallel report aggregation with `Mono.zip(...)`
* Scheduled jobs for unpaid payment expiration and outbox publishing
* Clean layered architecture
* DTO mapping with MapStruct
* Centralized validation and exception handling
* Operational admin APIs
* Dockerized local infrastructure
* Observability with Actuator, Micrometer, Prometheus, and Grafana
* GitHub Actions CI/CD with PostgreSQL and Kafka service containers
* PostgreSQL integration tests with Testcontainers
* Kafka Testcontainers integration tests with a real broker
* R2DBC repository testing against a real PostgreSQL database
* Service-layer integration tests against PostgreSQL
* Focused Kafka consumer tests for success, duplicate handling, failure propagation, and metrics
* Scheduler tests for operational background jobs
* Database-level validation of atomic inventory reservation and release logic
* Shared Testcontainers setup for PostgreSQL integration tests
* Test suite stabilization with R2DBC pool limits and disabled JUnit parallel execution

---

## Technology Stack

* Java 25
* Spring Boot
* Spring WebFlux
* Spring Data R2DBC
* PostgreSQL
* Apache Kafka
* Kafka UI
* Prometheus
* Grafana
* Docker Compose
* MapStruct
* Lombok
* SpringDoc OpenAPI / Swagger UI
* Spring Boot Actuator / Micrometer
* Maven
* JUnit 5
* Mockito
* Testcontainers
* GitHub Actions

---

## High-Level Architecture

The application is currently implemented as a modular monolith with clear bounded contexts. Each context can later be extracted into a separate microservice.

### Main Contexts

* Product Service
* User Service
* Order Service
* Inventory Service
* Payment / Billing Service
* Shipping / Tracking Service
* Audit Service
* Outbox Service
* Admin Service
* Reporting Module

### Service Summary

| Service           | Responsibility                                                                                             |
| ----------------- | ---------------------------------------------------------------------------------------------------------- |
| Product Service   | Manages the product catalog and product availability metadata used when orders are created.                |
| User Service      | Manages users, user status, blocking, activation, and validation before order creation.                    |
| Order Service     | Owns order creation, order status transitions, order items, and saga coordination through outbox events.   |
| Inventory Service | Reads inventory, atomically reserves/releases stock, and tracks reservation state for idempotent retries.  |
| Payment Service   | Creates pending payments from confirmed orders, completes or fails payments, and expires overdue payments. |
| Shipment Service  | Creates shipments from completed payments and manages shipment progress from created to delivered.         |
| Audit Service     | Persists lifecycle events consumed from Kafka into the audit log for traceability.                         |
| Outbox Service    | Stores domain events transactionally and publishes pending events to Kafka with retry support.             |
| Report Service    | Provides read-only operational reports and dashboard aggregations from PostgreSQL.                         |
| Admin Service     | Aggregates dashboard data and exposes operational views for users, audit events, and outbox events.        |

### Package Structure

```text
com.order
├── application
│   ├── mapper
│   └── service
│       └── impl
├── domain
│   ├── dto
│   │   ├── request
│   │   └── response
│   │       ├── admin
│   │       └── report
│   ├── entity
│   ├── enums
│   └── event
├── exception
├── infrastructure
│   ├── config
│   │   ├── converter
│   │   └── properties
│   ├── messaging
│   │   └── kafka
│   ├── observability
│   ├── repository
│   │   ├── custom
│   │   └── report
│   └── scheduler
└── presentation
    └── controller
        └── admin
```

---

## Architecture Overview

```mermaid
flowchart LR
    Client[Client / Swagger UI]

    Client --> Product[Product API]
    Client --> User[User API]
    Client --> Order[Order API]
    Client --> Payment[Payment API]
    Client --> Shipment[Shipment API]
    Client --> Reports[Reporting API]
    Client --> Admin[Admin API]

    Order --> Outbox[Outbox Service]
    Inventory --> Outbox
    Payment --> Outbox
    Shipment --> Outbox

    Outbox -->|publish pending events| Kafka[(Kafka)]

    Kafka -->|order.created| Inventory[Inventory Service]
    Kafka -->|inventory.reserved / inventory.failed| Order
    Kafka -->|order.confirmed| Payment[Payment Service]
    Kafka -->|payment.completed| Shipment[Shipping Service]
    Kafka -->|payment.failed / payment.expired| Order
    Kafka -->|order.cancelled| Inventory
    Kafka --> Audit[Audit Service]

    Product --> DB[(PostgreSQL)]
    User --> DB
    Order --> DB
    Inventory --> DB
    Payment --> DB
    Shipment --> DB
    Audit --> DB
    Reports --> DB
    Admin --> DB
    Outbox --> DB
```

### Runtime Layers

```text
Presentation layer
  REST controllers, request validation, OpenAPI annotations, centralized exception handling

Application layer
  Use-case services, reactive orchestration, transactions, DTO mapping

Domain layer
  Entities, enums, events, request/response DTOs

Infrastructure layer
  R2DBC repositories, custom SQL repositories, Kafka consumers, outbox publisher, schedulers, metrics, configuration
```

---

## Key Engineering Decisions

### Modular Monolith First

OrderFlow is intentionally implemented as a modular monolith. This keeps local development and deployment simple while still preserving clear bounded contexts around products, users, orders, inventory, payments, shipping, audit, reporting, and admin operations.

The package and service boundaries are designed so that individual contexts could later be extracted into separate services if operational needs justify the additional complexity.

### Transactional Outbox For Reliable Publishing

Business services do not publish directly to Kafka inside the main business flow. Instead, they persist domain events to `outbox_events` in the same reactive transaction as the related state change.

A scheduled publisher then publishes pending outbox events to Kafka and marks them as `PUBLISHED` or `FAILED`. This avoids the consistency problem where a database write succeeds but event publishing fails.

### Atomic SQL For Inventory Consistency

Inventory reservation uses conditional PostgreSQL updates instead of read-check-save logic. This keeps stock reservation safe under concurrent order creation because the availability check and quantity update happen in one atomic database operation.

### Idempotent Event Handling

Kafka consumers are designed to tolerate duplicate delivery. Inventory uses a reservation ledger with uniqueness constraints, and duplicate business events such as repeated `order.confirmed`, `order.cancelled`, or `payment.completed` messages are treated as already processed instead of creating duplicate payments, shipments, or stock movements.

### Reactive Composition Where It Adds Value

The project uses WebFlux, R2DBC, `Flux`, `Mono`, `TransactionalOperator`, and `Mono.zip(...)` to demonstrate non-blocking request handling, reactive database access, transactional orchestration, and parallel aggregation of independent report/admin queries.

### Operational Visibility

The admin, audit, outbox, reporting, metrics, Prometheus, and Grafana features are included to show how backend systems can be inspected and operated after business events have been processed.

---

## How to Run Locally

### 1. Start infrastructure

```bash
docker compose up -d
```

### 2. Verify containers

```bash
docker ps
```

Expected containers:

```text
postgres
kafka
kafka-ui
prometheus
grafana
```

### 3. Start the Spring Boot application

From the project root:

```bash
./mvnw spring-boot:run
```

Or run the main application class from the IDE.

### 4. Open tools

```text
Swagger UI: http://localhost:8081/swagger-ui.html
Kafka UI:   http://localhost:8080
Metrics:    http://localhost:8081/actuator/metrics
Prometheus: http://localhost:9090
Grafana:    http://localhost:3000
```

Grafana local credentials:

```text
Username: admin
Password: admin
```

---

## Observability And Metrics

Spring Boot Actuator exposes health, info, metrics, and Prometheus endpoints under `/actuator`.

Useful custom Kafka metrics:

* `orderflow.kafka.consumer.events` counts consumed Kafka records by `topic`, `outcome`, and `exception`.
  Outcomes are `success`, `duplicate`, and `failure`.
* `orderflow.kafka.dlt.events` counts records published to dead-letter topics by source topic, DLT topic, and exception.
* `orderflow.inventory.reservation.failures` counts inventory reservation failures that are converted into `INVENTORY_FAILED` outbox events.

Examples:

```bash
curl http://localhost:8081/actuator/metrics/orderflow.kafka.consumer.events
curl http://localhost:8081/actuator/metrics/orderflow.kafka.dlt.events
curl http://localhost:8081/actuator/metrics/orderflow.inventory.reservation.failures
curl http://localhost:8081/actuator/prometheus
```

Prometheus is configured in:

```text
config/prometheus/prometheus.yml
```

Grafana is provisioned automatically with:

```text
config/grafana/provisioning/datasources/prometheus.yml
config/grafana/dashboards/orderflow-observability.json
```

---

## CI/CD

GitHub Actions workflow:

```text
.github/workflows/ci-cd.yml
```

The pipeline runs on:

* Pull requests to `main` or `master`
* Pushes to `main` or `master`
* Manual `workflow_dispatch`

The CI workflow validates the application against real infrastructure dependencies by starting PostgreSQL and Kafka as GitHub Actions service containers.

Pipeline stages:

```text
Checkout
   ↓
Set up JDK 25 with Maven cache
   ↓
Start PostgreSQL service container
   ↓
Start Kafka service container
   ↓
Run ./mvnw --batch-mode clean verify -Dspring.test.context.cache.maxSize=4
   ↓
Upload test reports
   ↓
Upload packaged application JAR on main/master pushes
```

The workflow disables Spring Boot Docker Compose integration in CI and points the application to the GitHub Actions service containers:

```text
SPRING_DOCKER_COMPOSE_ENABLED=false
SPRING_R2DBC_URL=r2dbc:postgresql://localhost:5433/orderflow
SPRING_R2DBC_USERNAME=postgres
SPRING_R2DBC_PASSWORD=postgres
SPRING_SQL_INIT_MODE=always
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ORDERFLOW_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

The Maven command also limits the Spring TestContext cache size:

```bash
./mvnw --batch-mode clean verify -Dspring.test.context.cache.maxSize=4
```

This helps prevent PostgreSQL connection exhaustion in the full integration-test suite when multiple Spring contexts and R2DBC pools are created.

The current delivery step publishes the built JAR as a GitHub Actions artifact. A real deployment target can be added later when the hosting environment is chosen.

This CI/CD setup is intentionally lightweight but practical for a portfolio backend project: every pull request and main-branch change must compile, run tests, connect to PostgreSQL, connect to Kafka, and complete Maven verification successfully.

---

## Testing Strategy

The project includes a layered automated testing strategy designed to validate persistence, business workflows, transactional behavior, scheduled jobs, and event-driven reliability.

The full suite can be executed locally with:

```bash
./mvnw clean test
```

The CI workflow executes:

```bash
./mvnw --batch-mode clean verify -Dspring.test.context.cache.maxSize=4
```

### Test Runtime Settings

The test suite includes test-specific configuration under `src/test/resources`.

Recommended R2DBC pool settings for test execution:

```yaml
spring:
  r2dbc:
    pool:
      enabled: true
      initial-size: 0
      max-size: 2
      max-idle-time: 30s
      max-acquire-time: 5s
      max-create-connection-time: 5s
```

JUnit parallel execution is disabled for stability:

```properties
junit.jupiter.execution.parallel.enabled=false
```

These settings keep the test suite stable when repository tests, service-layer integration tests, scheduler tests, Kafka consumer tests, and Kafka Testcontainers tests are executed together.

### Repository and Database Integration Tests

PostgreSQL integration tests use Testcontainers and run against a real PostgreSQL database instead of mocks or an in-memory replacement.

Covered areas include:

* Reactive repository persistence for products, users, orders, order items, inventory, payments, shipments, audit events, and outbox events
* Custom repository queries using `R2dbcEntityTemplate`
* Pagination and sorting behavior
* Atomic inventory stock mutation queries
* Query methods such as `findByOrderId`, `existsByOrderId`, `findByProductId`, and status-based filtering
* Reporting queries and SQL aggregations, including revenue, inventory, payment, outbox, and top-product reports

### Service-Layer Integration Tests

Core business services are tested with a real PostgreSQL database and mocked external publishing boundaries where appropriate.

Covered service flows include:

* Order creation with item persistence, total calculation, validation, and outbox event creation
* Order confirmation and cancellation state transitions
* Event-driven order updates from inventory and payment outcomes
* Inventory reservation and release flows
* Idempotent handling of duplicate inventory reservation and release events
* Payment creation, completion, failure, and overdue expiration
* Transactional rollback when outbox persistence fails
* Real database state verification after each business operation

### Transactional Outbox Tests

The project uses the Transactional Outbox Pattern to reliably persist domain events before publishing them to Kafka.

Outbox tests verify that:

* Events are saved with `PENDING` status
* Event metadata is persisted correctly
* Payloads are serialized as JSON
* Retry metadata is initialized and updated correctly
* Publishable events are selected according to status and retry limits
* Published and failed events are handled consistently

### Scheduler Tests

Scheduler tests focus on orchestration logic rather than duplicating service-layer behavior.

Covered schedulers include:

* Unpaid payment expiration scheduler
* Outbox publisher scheduler

The tests verify that schedulers:

* Do not run when disabled
* Use configured properties such as expiration days and max retry count
* Invoke the correct service methods when enabled
* Subscribe to reactive flows
* Handle service errors without throwing exceptions to the scheduler caller

### Focused Kafka Consumer Tests

Focused Kafka consumer tests verify consumer behavior without starting a real Kafka broker.

Covered areas include:

* Successful consumer processing for important business events
* Duplicate business event handling, such as duplicate `order.confirmed` and `payment.completed` messages
* Propagation of unexpected failures so Kafka retry policies can handle transient processing errors
* Conversion of inventory reservation business failures into `INVENTORY_FAILED` outbox events
* Consumer outcome metrics for `success`, `duplicate`, and `failure`
* Inventory reservation failure metrics

### Kafka Testcontainers Integration Tests

Kafka Testcontainers tests validate selected flows against a real Kafka broker instead of mocking the messaging layer.

Covered Kafka integration scenarios include:

* Producer and consumer smoke test against a real Kafka container
* Transactional outbox publishing to a real Kafka topic
* `order.confirmed` consumed by the real `PaymentOrderConfirmedConsumer`, creating a pending payment and a `PAYMENT_CREATED` outbox event
* `payment.completed` consumed by the real `PaymentEventConsumer`, creating a shipment and a `SHIPMENT_CREATED` outbox event
* `payment.failed` consumed by the real `PaymentEventConsumer`, cancelling the order and creating an `ORDER_CANCELLED` outbox event
* `payment.expired` consumed by the real `PaymentEventConsumer`, cancelling the order and creating an `ORDER_CANCELLED` outbox event
* `order.created` consumed by the real `InventoryOrderEventConsumer`, reserving inventory and creating an `INVENTORY_RESERVED` outbox event
* `order.created` with insufficient stock consumed by the real `InventoryOrderEventConsumer`, creating an `INVENTORY_FAILED` outbox event
* `order.cancelled` consumed by the real `InventoryOrderEventConsumer`, releasing inventory and creating an `INVENTORY_RELEASED` outbox event

The service-layer integration tests complement the Kafka tests by verifying idempotent business behavior, transactional rollback on outbox failures, and state consistency in PostgreSQL.

### CI Test Environment

The GitHub Actions workflow starts PostgreSQL and Kafka service containers before running Maven verification. Spring Boot Docker Compose integration is disabled in CI, and the application is pointed directly to the GitHub Actions service containers.

This keeps the CI setup lightweight while still validating the application against infrastructure that is close to the local development environment.

---

## Local Infrastructure

The project uses Docker Compose for local infrastructure.

### Services

* PostgreSQL
* Kafka
* Kafka UI
* Prometheus
* Grafana

### Ports

```text
PostgreSQL: 5433
Kafka:      9092
Kafka UI:   8080
Prometheus: 9090
Grafana:    3000
Application: 8081
```

---

## End-to-End Business Flow

### Happy Path

```mermaid
sequenceDiagram
    participant C as Client
    participant O as Order Service
    participant OB as Outbox
    participant K as Kafka
    participant I as Inventory Service
    participant P as Payment Service
    participant S as Shipment Service

    C->>O: POST /orders
    O->>OB: save ORDER_CREATED
    OB->>K: publish order.created
    K->>I: order.created
    I->>OB: save INVENTORY_RESERVED
    OB->>K: publish inventory.reserved
    K->>O: inventory.reserved
    O->>OB: save ORDER_CONFIRMED
    OB->>K: publish order.confirmed
    K->>P: order.confirmed
    P->>OB: save PAYMENT_CREATED
    OB->>K: publish payment.created
    C->>P: PATCH /payments/{id}/complete
    P->>OB: save PAYMENT_COMPLETED
    OB->>K: publish payment.completed
    K->>S: payment.completed
    S->>OB: save SHIPMENT_CREATED
    OB->>K: publish shipment.created
```

### Compensation Path

```mermaid
sequenceDiagram
    participant O as Order Service
    participant OB as Outbox
    participant K as Kafka
    participant I as Inventory Service
    participant P as Payment Service

    O->>OB: save ORDER_CREATED
    OB->>K: publish order.created
    K->>I: order.created
    I->>OB: save INVENTORY_RESERVED
    OB->>K: publish inventory.reserved
    K->>O: inventory.reserved
    O->>OB: save ORDER_CONFIRMED
    OB->>K: publish order.confirmed
    K->>P: order.confirmed
    P->>OB: save PAYMENT_FAILED / PAYMENT_EXPIRED
    OB->>K: publish payment.failed / payment.expired
    K->>O: payment.failed / payment.expired
    O->>OB: save ORDER_CANCELLED
    OB->>K: publish order.cancelled
    K->>I: order.cancelled
    I->>OB: save INVENTORY_RELEASED
    OB->>K: publish inventory.released
```

---

## Core Business Flow

### Order Creation Flow

```text
POST /api/v1/orders
   ↓
Order CREATED
   ↓
ORDER_CREATED outbox event
   ↓
OutboxPublisherScheduler publishes order.created
   ↓
Inventory reserves stock
   ↓
INVENTORY_RESERVED or INVENTORY_FAILED outbox event
```

If inventory is reserved successfully:

```text
inventory.reserved
   ↓
Order CONFIRMED
   ↓
ORDER_CONFIRMED outbox event
   ↓
OutboxPublisherScheduler publishes order.confirmed
   ↓
Payment PENDING is created
   ↓
PAYMENT_CREATED outbox event
```

If inventory reservation fails:

```text
inventory.failed
   ↓
Order FAILED
```

---

## Payment Flow

After an order is confirmed, a payment is created with status `PENDING`.

```text
order.confirmed
   ↓
PaymentOrderConfirmedConsumer
   ↓
Payment PENDING
   ↓
PAYMENT_CREATED outbox event
   ↓
payment.created
```

Payment can then be completed manually:

```text
PATCH /api/v1/payments/{id}/complete
   ↓
Payment COMPLETED
   ↓
PAYMENT_COMPLETED outbox event
   ↓
payment.completed
   ↓
Shipment CREATED
```

Or failed manually:

```text
PATCH /api/v1/payments/{id}/fail
   ↓
Payment FAILED
   ↓
PAYMENT_FAILED outbox event
   ↓
payment.failed
   ↓
Order CANCELLED
   ↓
ORDER_CANCELLED outbox event
   ↓
order.cancelled
   ↓
Inventory RELEASED
```

---

## Scheduled Payment Expiration

The system includes a scheduler that expires unpaid payments after a configured number of days.

```text
Payment PENDING
   ↓ after configured expiration period
UnpaidPaymentScheduler
   ↓
Payment EXPIRED
   ↓
PAYMENT_EXPIRED outbox event
   ↓
payment.expired
   ↓
Order CANCELLED
   ↓
ORDER_CANCELLED outbox event
   ↓
order.cancelled
   ↓
Inventory RELEASED
```

Example configuration:

```yaml
orderflow:
  scheduler:
    unpaid-payments:
      enabled: true
      expiration-days: 3
      fixed-delay-ms: 3600000
```

For local testing:

```yaml
orderflow:
  scheduler:
    unpaid-payments:
      enabled: true
      expiration-days: 0
      fixed-delay-ms: 30000
```

---

## Shipping Flow

Shipping starts only after payment is completed.

```text
payment.completed
   ↓
PaymentEventConsumer
   ↓
Shipment CREATED
   ↓
SHIPMENT_CREATED outbox event
   ↓
shipment.created
```

Shipment status transitions:

```text
CREATED -> SHIPPED -> DELIVERED
```

Each shipment transition is also published through the outbox:

```text
SHIPMENT_CREATED   -> shipment.created
SHIPMENT_SHIPPED   -> shipment.shipped
SHIPMENT_DELIVERED -> shipment.delivered
```

Endpoints:

```http
GET   /api/v1/shipments/{id}
GET   /api/v1/shipments/orders/{orderId}
PATCH /api/v1/shipments/{id}/ship
PATCH /api/v1/shipments/{id}/deliver
```

---

## Inventory Flow

Inventory is the source of truth for available and reserved stock.

Inventory mutations are concurrency-safe at the database level. Reservation and release operations use conditional PostgreSQL updates instead of read-check-save logic:

```sql
UPDATE inventory
SET available_quantity = available_quantity - :quantity,
    reserved_quantity = reserved_quantity + :quantity
WHERE product_id = :productId
  AND available_quantity >= :quantity;
```

The row update and stock check happen atomically, so two concurrent orders cannot both reserve the same available units.

Inventory also keeps an `inventory_reservations` ledger with a unique `(order_id, product_id)` constraint. This protects the service from duplicate Kafka deliveries:

```text
Duplicate order.created
   ↓
inventory_reservations insert conflicts
   ↓
stock is not reserved a second time
```

Duplicate `order.cancelled` events are also idempotent: if the matching reservation is already marked `RELEASED`, the service returns the release event without moving stock again.

### Reservation

```text
order.created
   ↓
InventoryOrderEventConsumer
   ↓
insert inventory_reservations row
   ↓
reserve inventory using atomic SQL update
   ↓
INVENTORY_RESERVED outbox event
   ↓
inventory.reserved
```

### Reservation Failure

```text
order.created
   ↓
InventoryOrderEventConsumer
   ↓
insert reservation attempt
   ↓
reserve inventory using atomic SQL update
   ↓
updated rows = 0
   ↓
InventoryReservationException
   ↓
INVENTORY_FAILED outbox event
   ↓
inventory.failed
```

### Release

```text
order.cancelled
   ↓
InventoryOrderEventConsumer
   ↓
mark inventory_reservations row as RELEASED
   ↓
release reserved inventory
   ↓
INVENTORY_RELEASED outbox event
   ↓
inventory.released
```

The system uses reactive composition to process order items:

```java
Flux.fromIterable(event.items())
        .flatMap(this::reserveSingleItem)
    .collectList();
```

---

## Kafka Topics

### Order Topics

```text
order.created
order.confirmed
order.cancelled
```

### Inventory Topics

```text
inventory.reserved
inventory.failed
inventory.released
```

### Payment Topics

```text
payment.created
payment.completed
payment.failed
payment.expired
```

### Shipment Topics

```text
shipment.created
shipment.shipped
shipment.delivered
```

---

## Kafka Consumer Groups

```yaml
orderflow:
  kafka:
    consumer-groups:
      audit: orderflow-audit-service
      inventory: orderflow-inventory-service
      order: orderflow-order-service
      shipment: orderflow-shipment-service
      payment: orderflow-payment-service
```

Different consumer groups allow multiple bounded contexts to react to the same event independently.

For example, both Audit and Inventory consume `order.created`, but they use different consumer groups.

Kafka listeners wait for their reactive database and outbox work to finish before returning to the container. This keeps Kafka retry semantics aligned with the actual processing result: if the database write or outbox save fails, the listener throws and the container retry policy handles the record.

Duplicate business events that are already safely handled by database constraints are treated as processed. For example, duplicate `order.confirmed` events do not create a second payment, duplicate `payment.completed` events do not create a second shipment, and duplicate `order.cancelled` events do not release stock twice.

---

## Kafka Retry And DLT

Kafka consumer failures use a bounded retry policy:

```text
process record
   ↓
retry after 1 second
   ↓
retry after 1 second
   ↓
retry after 1 second
   ↓
publish original record to <topic>.DLT
```

Invalid payloads are not retried. They are treated as non-retryable and are sent to the matching dead-letter topic immediately.

Dead-letter topics are created for each application topic using the `<topic>.DLT` naming convention, for example:

```text
order.created.DLT
inventory.reserved.DLT
payment.completed.DLT
```

---

## Kafka Topic Creation

Kafka topics are declared through Spring Kafka topic configuration instead of relying only on broker auto-creation.

Example configuration:

```yaml
orderflow:
  kafka:
    topic-settings:
      partitions: 1
      replicas: 1
```

For local development, one partition and one replica are sufficient because the Docker setup uses a single Kafka broker.

---

## Transactional Outbox Pattern

Business services write domain events to `outbox_events` inside the same reactive transaction as the related state change.

This prevents the common consistency problem where a database change succeeds, but the Kafka publish fails.

```text
Business transaction
   ↓
Save aggregate changes
   ↓
Save outbox event in the same transaction
   ↓
Commit
   ↓
OutboxPublisherScheduler polls publishable events
   ↓
Publish to Kafka
   ↓
Mark event as PUBLISHED or FAILED
```

The following lifecycle events are published through the outbox:

```text
ORDER_CREATED
ORDER_CONFIRMED
ORDER_CANCELLED

INVENTORY_RESERVED
INVENTORY_FAILED
INVENTORY_RELEASED

PAYMENT_CREATED
PAYMENT_COMPLETED
PAYMENT_FAILED
PAYMENT_EXPIRED

SHIPMENT_CREATED
SHIPMENT_SHIPPED
SHIPMENT_DELIVERED
```

Outbox publishing is configurable:

```yaml
orderflow:
  scheduler:
    outbox:
      enabled: true
      fixed-delay-ms: 5000
      max-retries: 5
```

Failed outbox events can be inspected and manually retried through the admin API.

Only `FAILED` outbox events can be manually retried to avoid duplicate publishing of already `PUBLISHED` events.

---

## Reporting Module

The reporting module is read-only and uses custom SQL aggregation queries via `DatabaseClient`.

### Report Endpoints

```http
GET /api/v1/reports/orders/summary
GET /api/v1/reports/revenue
GET /api/v1/reports/inventory
GET /api/v1/reports/payments
GET /api/v1/reports/top-products
GET /api/v1/reports/dashboard
```

### Dashboard Report

The dashboard combines independent report queries in parallel using `Mono.zip(...)`:

```java
Mono.zip(
        ordersMono,
        revenueMono,
        inventoryMono,
        paymentsMono,
        topProductsMono
        )
```

This demonstrates one of the practical advantages of reactive programming: independent non-blocking operations can be composed and resolved together.

---

## Admin Module

The admin module exposes operational views over the system without taking ownership of the core business workflows.

### Admin Dashboard

```http
GET /api/v1/admin/dashboard
```

The dashboard is served by `AdminService` and combines order, payment, revenue, inventory, top-product, and outbox summaries through parallel repository calls.

### Admin Audit Events

`AdminAuditController` exposes paged access to stored audit events:

```http
GET /api/v1/admin/audit-events
GET /api/v1/admin/audit-events/{id}
GET /api/v1/admin/audit-events/orders/{orderId}
```

These endpoints are read-only and delegate to `AdminService`, which reads `audit_events`, validates page and size limits, and returns `PagedResponse<AuditEventResponse>`.

### Admin Outbox Events

```http
GET   /api/v1/admin/outbox-events
GET   /api/v1/admin/outbox-events/{id}
PATCH /api/v1/admin/outbox-events/{id}/retry
```

Outbox events can be listed, filtered by status, inspected by id, and manually moved from `FAILED` back to `PENDING` for retry.

Only `FAILED` events can be retried manually.

### Admin User Actions

```http
PATCH /api/v1/admin/users/{id}/block
PATCH /api/v1/admin/users/{id}/activate
```

These endpoints reuse `UserService` so administrative status changes follow the same validation rules as the rest of the user domain.

---

## Main REST API Overview

### Products

```http
POST   /api/v1/products
GET    /api/v1/products
GET    /api/v1/products/{id}
PUT    /api/v1/products/{id}
DELETE /api/v1/products/{id}
```

### Users

```http
POST   /api/v1/users
GET    /api/v1/users
GET    /api/v1/users/{id}
PUT    /api/v1/users/{id}
DELETE /api/v1/users/{id}
```

### Orders

```http
POST  /api/v1/orders
GET   /api/v1/orders
GET   /api/v1/orders/{id}
PATCH /api/v1/orders/{id}/cancel
```

### Inventory

```http
GET /api/v1/inventory/products
GET /api/v1/inventory/products/{productId}
```

### Payments

```http
GET   /api/v1/payments/{id}
GET   /api/v1/payments/orders/{orderId}
PATCH /api/v1/payments/{id}/complete
PATCH /api/v1/payments/{id}/fail
```

### Shipments

```http
GET   /api/v1/shipments/{id}
GET   /api/v1/shipments/orders/{orderId}
PATCH /api/v1/shipments/{id}/ship
PATCH /api/v1/shipments/{id}/deliver
```

### Reports

```http
GET /api/v1/reports/orders/summary
GET /api/v1/reports/revenue
GET /api/v1/reports/inventory
GET /api/v1/reports/payments
GET /api/v1/reports/top-products
GET /api/v1/reports/dashboard
```

### Admin

```http
GET   /api/v1/admin/dashboard
GET   /api/v1/admin/audit-events
GET   /api/v1/admin/audit-events/{id}
GET   /api/v1/admin/audit-events/orders/{orderId}
GET   /api/v1/admin/outbox-events
GET   /api/v1/admin/outbox-events/{id}
PATCH /api/v1/admin/outbox-events/{id}/retry
PATCH /api/v1/admin/users/{id}/block
PATCH /api/v1/admin/users/{id}/activate
```

---

## Order Lifecycle

Current order statuses:

```text
CREATED
CONFIRMED
CANCELLED
FAILED
```

Main transitions:

```text
CREATED -> CONFIRMED      when inventory is reserved
CREATED -> FAILED         when inventory reservation fails
CONFIRMED -> CANCELLED    when payment fails or expires
CONFIRMED -> CANCELLED    when order is manually cancelled
```

---

## Payment Lifecycle

Payment statuses:

```text
PENDING
COMPLETED
FAILED
EXPIRED
```

Main transitions:

```text
PENDING -> COMPLETED
PENDING -> FAILED
PENDING -> EXPIRED
```

---

## Shipment Lifecycle

Shipment statuses:

```text
CREATED
SHIPPED
DELIVERED
CANCELLED
```

Main transitions:

```text
CREATED -> SHIPPED -> DELIVERED
```

---

## User Lifecycle

User roles:

```text
CUSTOMER
ADMIN
```

User statuses:

```text
ACTIVE
BLOCKED
DELETED
```

Blocked or deleted users cannot create new orders.

Admin endpoints can block or reactivate users.

---

## Reactive Highlights

### Parallel Inventory Reservation

Inventory reservation processes order items reactively:

```java
Flux.fromIterable(event.items())
        .flatMap(this::reserveSingleItem)
    .collectList();
```

### Parallel Dashboard Aggregation

Dashboard reporting combines independent queries with `Mono.zip(...)`:

```java
return Mono.zip(
        ordersMono,
        revenueMono,
        inventoryMono,
        paymentsMono,
        topProductsMono
        )
.map(tuple -> new DashboardReportResponse(...));
```

### Parallel Admin Dashboard Aggregation

The admin dashboard also combines multiple independent operational queries:

```java
return Mono.zip(
        ordersMono,
        paymentsMono,
        revenueMono,
        inventoryMono,
        topProductsMono,
        outboxMono
        )
.map(tuple -> new AdminDashboardResponse(...));
```

### Reactive Transactions

Important business operations are wrapped in reactive transactions using `TransactionalOperator`.

Examples:

* order creation
* inventory reservation
* inventory release
* inventory reservation idempotency
* order confirmation from inventory event
* order cancellation from payment failure
* payment creation
* payment completion
* payment failure
* payment expiration
* shipment creation
* shipment shipping
* shipment delivery

---

## Audit Logging

The Audit Service listens to lifecycle events and stores them in the `audit_events` table.

Examples:

```text
ORDER_CREATED
ORDER_CONFIRMED
ORDER_CANCELLED
```

The audit log stores:

```text
event_type
aggregate_type
aggregate_id
payload
created_at
```

Audit events can be inspected through admin endpoints.

---

## Configuration Philosophy

The project avoids hardcoded infrastructure settings.

Kafka topics, consumer groups, topic settings, report limits, scheduler settings, test runtime settings, and other configurable values are defined in YAML configuration and loaded through `@ConfigurationProperties` classes.

Examples:

```yaml
orderflow:
  kafka:
    bootstrap-servers: localhost:9092
    topic-settings:
      partitions: 1
      replicas: 1
```

```yaml
orderflow:
  reports:
    dashboard:
      top-products-limit: 5
```

```yaml
orderflow:
  scheduler:
    unpaid-payments:
      enabled: true
      expiration-days: 3
      fixed-delay-ms: 3600000
    outbox:
      enabled: true
      fixed-delay-ms: 5000
      max-retries: 5
```

For Kubernetes or production-like environments, these values can be overridden through environment variables while local defaults remain usable for development.

---

## Example End-to-End Happy Path

```text
1. Create products and users
2. Create inventory records for products
3. Create an order
4. ORDER_CREATED is saved to the outbox
5. OutboxPublisherScheduler publishes order.created
6. Inventory creates reservation ledger rows and atomically reserves stock
7. INVENTORY_RESERVED is saved to the outbox
8. OutboxPublisherScheduler publishes inventory.reserved
9. Order status becomes CONFIRMED
10. ORDER_CONFIRMED is saved to the outbox
11. Payment is created with PENDING status
12. PAYMENT_CREATED is saved to the outbox
13. Payment is completed manually
14. PAYMENT_COMPLETED is saved to the outbox
15. Shipment is created
16. SHIPMENT_CREATED is saved to the outbox
17. Shipment is marked as SHIPPED
18. Shipment is marked as DELIVERED
19. Open reporting dashboard
20. Open admin dashboard
21. Inspect outbox events
22. Inspect audit events for the created order
```

---

## Example Failure / Compensation Path

```text
1. Create an order
2. Inventory creates reservation ledger rows and atomically reserves stock
3. Payment is created with PENDING status
4. Payment fails or expires
5. PAYMENT_FAILED or PAYMENT_EXPIRED is saved to the outbox
6. OutboxPublisherScheduler publishes payment.failed or payment.expired
7. Order is cancelled
8. ORDER_CANCELLED is saved to the outbox
9. OutboxPublisherScheduler publishes order.cancelled
10. Inventory marks reservation ledger rows as RELEASED and atomically releases stock
11. INVENTORY_RELEASED is saved to the outbox
12. OutboxPublisherScheduler publishes inventory.released
13. Check audit events for the cancelled order
14. Check outbox events and confirm all related events are PUBLISHED
```

---

## Portfolio Highlights

This project demonstrates several backend engineering concepts that are useful in real-world systems:

* Reactive REST APIs with WebFlux
* Non-blocking PostgreSQL access with R2DBC
* Event-driven business workflows with Kafka
* Transactional Outbox Pattern for reliable event publishing
* Saga-like compensation through events
* Concurrent-safe inventory reservation and release logic
* Idempotent handling of duplicate inventory events
* Payment expiration through scheduled jobs
* Reporting read models with custom SQL
* Parallel dashboard aggregation with `Mono.zip(...)`
* Operational admin APIs for outbox and audit inspection
* Observability with Prometheus and Grafana
* Centralized configuration through YAML
* Modular monolith structure ready for microservice extraction
* GitHub Actions CI/CD with PostgreSQL and Kafka service containers
* PostgreSQL integration tests with Testcontainers
* Kafka Testcontainers coverage for real broker publishing and selected listener flows
* Full test-suite stabilization for larger integration-test runs

---

## Recommended Demo Scenario

A good demo sequence for the project is:

```text
1. Create products and users
2. Create inventory records for products
3. Create an order
4. Watch ORDER_CREATED in outbox_events
5. Watch order.created in Kafka UI
6. Watch INVENTORY_RESERVED and ORDER_CONFIRMED through outbox_events
7. Check payment PENDING
8. Complete payment manually
9. Watch PAYMENT_COMPLETED and SHIPMENT_CREATED through outbox_events
10. Mark shipment as SHIPPED and DELIVERED
11. Open reporting dashboard
12. Open admin dashboard
13. Inspect outbox events
14. Inspect audit events for the created order
```

Failure scenario:

```text
1. Create an order
2. Let payment stay PENDING
3. Scheduler expires the payment
4. PAYMENT_EXPIRED is saved to the outbox
5. payment.expired is published
6. Order is cancelled
7. Inventory marks reservation ledger rows as RELEASED and releases stock
8. Check audit events for the cancelled order
9. Check outbox events and confirm all related events are PUBLISHED
```

Inventory failure scenario:

```text
1. Create product inventory with low stock
2. Create an order requesting more units than available
3. Inventory reservation fails
4. INVENTORY_FAILED is saved to the outbox
5. inventory.failed is published
6. Order moves to FAILED
7. Inspect outbox and audit data
```

---

## Current Limitations And Production Notes

This project is production-inspired, but it is still a portfolio project and not a complete production system.

Current intentional limitations:

* Authentication and authorization are not implemented yet.
* Admin endpoints are separated by route and service boundaries, but they are not protected by Spring Security yet.
* The application currently runs as a single modular monolith deployment.
* Kafka topics use one partition and one replica in local development.
* The current CI/CD workflow builds, tests, and publishes a JAR artifact, but it does not deploy to a live hosting environment.
* Distributed tracing and correlation IDs across HTTP, Kafka, outbox, and database operations are planned but not yet implemented.

These limitations are documented explicitly because the main purpose of the project is to demonstrate backend architecture, reliability patterns, reactive programming, event-driven workflows, testing discipline, and operational visibility.

---

## Build Status

The project is verified by GitHub Actions on every push and pull request to `main` and `master`.

The CI workflow starts PostgreSQL and Kafka service containers, disables local Docker Compose integration, runs Maven `clean verify` with a limited Spring TestContext cache, uploads test reports, and publishes the application JAR artifact on successful pushes.

---

## Future Improvements

Potential next steps:

* End-to-end verification of Kafka retry and dead-letter topic behavior with real Kafka
* Contract tests for Kafka event schemas before extracting bounded contexts into services
* Separate modules or microservices per bounded context
* Authentication and authorization with Spring Security
* More advanced reporting and time-based analytics
* Distributed tracing with correlation IDs across HTTP, outbox, Kafka, and database work
* Admin workflow for inspecting, replaying, or parking records from dead-letter topics
* Docker image publishing and deployment to a real hosting environment

---

## Current Status

Implemented:

* Product CRUD
* User CRUD
* User role and status management
* Admin user block and activate actions
* Order lifecycle
* Inventory reservation and release
* Atomic inventory stock updates
* Inventory reservation ledger for duplicate event protection
* Payment lifecycle
* Scheduled payment expiration
* Shipping lifecycle
* Kafka consumer flows
* Kafka consumer retry and dead-letter topics
* Reliable Kafka publishing through transactional outbox
* Outbox event persistence and scheduled publishing
* Outbox retry support
* Audit event persistence
* Admin dashboard
* Admin audit and outbox endpoints
* Reporting dashboard
* Top products report
* Spring Boot Actuator metrics endpoint
* Custom Micrometer metrics for Kafka consumer outcomes, DLT publishing, and inventory reservation failures
* Prometheus scrape configuration
* Grafana datasource provisioning and OrderFlow reliability dashboard
* GitHub Actions CI/CD workflow for Maven verification and JAR artifact publishing
* PostgreSQL integration tests with Testcontainers
* Shared PostgreSQL Testcontainers base setup for integration tests
* Repository integration tests for products, users, orders, order items, inventory, outbox events, audit events, payments, and shipments
* Inventory stock mutation integration tests for reserve and release operations with `rowsUpdated` verification
* Service-layer integration tests for order, inventory, payment, and outbox workflows
* Scheduler tests for unpaid payment expiration and outbox publishing
* Focused Kafka consumer tests for successful processing, duplicate handling, failure propagation, and consumer metrics
* Kafka Testcontainers smoke test for real broker producer/consumer communication
* Kafka Testcontainers outbox publishing test with real Kafka topic verification
* Kafka Testcontainers listener test for `order.confirmed -> payment created`
* Kafka Testcontainers listener tests for `payment.completed`, `payment.failed`, and `payment.expired` flows
* Kafka Testcontainers listener tests for `order.created -> inventory reserved`, `order.created -> inventory failed`, and `order.cancelled -> inventory released`
* Test suite stabilization through test-specific R2DBC pool settings and disabled JUnit parallel execution
* Swagger/OpenAPI support
* Docker-based local infrastructure
