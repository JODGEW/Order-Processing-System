# Order Processing System

An event-driven order processing backend built with Spring Boot, PostgreSQL, and Apache Kafka. Demonstrates production patterns including transactional outbox, consumer idempotency, optimistic locking, and a state machine for order lifecycle management.

## Architecture

```
                         ┌──────────────┐
   HTTP POST /orders ──> │ Order Service│──> PostgreSQL
                         └──────┬───────┘
                                │
                        Kafka: order-events
                                │
                   ┌────────────┼────────────┐
                   v                         v
          ┌────────────────┐       ┌────────────────────┐
          │ Payment Service│       │ Notification Service│
          └───────┬────────┘       └────────────────────┘
                  │
          Kafka: payment-events
                  │
                  v
        ┌───────────────────┐
        │ OrderStatusUpdater│──> Updates order status
        └───────────────────┘
```

- **Order Service** -- REST API, persists orders to Postgres, publishes `OrderCreated` events via a transactional outbox.
- **Payment Service** -- Consumes `OrderCreated` events, simulates payment (80% success), publishes `PaymentCompleted` or `PaymentFailed`.
- **Notification Service** -- Consumes events from both topics, logs notifications (simulates email/SMS).
- **OrderStatusUpdater** -- Consumes payment results and transitions order status via a state machine.

## Tech Stack

| Layer            | Technology                  |
|------------------|-----------------------------|
| Language         | Java 17                     |
| Framework        | Spring Boot 3.5             |
| Database         | PostgreSQL 16               |
| Messaging        | Apache Kafka (KRaft)        |
| ORM              | Spring Data JPA / Hibernate |
| Containerization | Docker & Docker Compose     |
| Build            | Maven                       |

## Getting Started

### Prerequisites

- Java 17+
- Docker & Docker Compose

### Run the full stack

```bash
./mvnw clean package -DskipTests
docker compose up --build
```

The dashboard is available at **http://localhost:8080/** -- a single-page UI for creating orders, viewing status transitions, and inspecting order details.

### Development mode

Run Postgres and Kafka in Docker, the app locally:

```bash
docker compose up postgres kafka
./mvnw spring-boot:run
```

## Dashboard

A vanilla HTML/CSS/JS frontend served at the root path (`/`) -- no build tools or frameworks.

- Create orders with dynamic item rows
- Auto-refreshing order list with color-coded status badges (green = PAID, yellow = PENDING, red = FAILED)
- Click any order to see item breakdown and timestamps
- Connection status indicator

## API

| Method | Path           | Description        |
|--------|----------------|--------------------|
| POST   | `/orders`      | Create a new order |
| GET    | `/orders`      | List all orders    |
| GET    | `/orders/{id}` | Get order by ID    |

### Example requests

```bash
# Create an order
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-1",
    "idempotencyKey": "abc-123",
    "items": [
      {"productName": "Widget", "quantity": 2, "unitPrice": 14.99}
    ]
  }' | jq

# List orders (status transitions from PENDING -> PAID/PAYMENT_FAILED)
curl -s http://localhost:8080/orders | jq

# Get by ID
curl -s http://localhost:8080/orders/{id} | jq
```

## Data Model

```
┌───────────────────────┐        ┌───────────────────────┐
│        orders         │        │      order_items       │
├───────────────────────┤        ├───────────────────────┤
│ id          (UUID PK) │───┐    │ id          (UUID PK) │
│ user_id     (VARCHAR) │   │    │ order_id    (UUID FK) │
│ status      (VARCHAR) │   └───>│ product_name(VARCHAR) │
│ total_price (DECIMAL) │        │ quantity    (INT)     │
│ idempotency_key (UQ)  │        │ unit_price  (DECIMAL) │
│ version     (INT)     │        └───────────────────────┘
│ created_at  (TIMESTAMP)│
│ updated_at  (TIMESTAMP)│
└───────────────────────┘

┌───────────────────────┐       ┌───────────────────────┐
│     outbox_events     │       │   processed_events    │
├───────────────────────┤       ├───────────────────────┤
│ id          (UUID PK) │       │ event_id   (VARCHAR)  │
│ aggregate_id(VARCHAR) │       │ consumer_group(VARCHAR)│
│ event_type  (VARCHAR) │       │ processed_at(TIMESTAMP)│
│ partition_key(VARCHAR)│       └───────────────────────┘
│ payload     (TEXT)    │
│ topic       (VARCHAR) │
│ created_at  (TIMESTAMP)│
│ published   (BOOLEAN) │
└───────────────────────┘
```

## Kafka Topics

| Topic            | Producer        | Consumers                                   |
|------------------|-----------------|---------------------------------------------|
| `order-events`   | Order Service   | Payment Service, Notification Service       |
| `payment-events` | Payment Service | OrderStatusUpdater, Notification Service    |

## Key Design Decisions

| Decision | Approach | Rationale |
|----------|----------|-----------|
| Event publishing | Transactional outbox | Order + event written in one DB transaction -- eliminates dual-write inconsistency |
| Consumer idempotency | `processed_events` table | `(event_id, consumer_group)` unique constraint checked in the same transaction as business logic -- handles Kafka at-least-once redelivery |
| Concurrency control | `@Version` optimistic locking | Prevents lost updates from concurrent consumers updating order status |
| State transitions | Enum state machine | `OrderStatus.canTransitionTo()` rejects invalid transitions (e.g., `PAID -> PENDING`) at the domain level |
| Partition key | `userId` | All events for a user land on the same partition -- guarantees per-user ordering |
| Retry policy | `FixedBackOff` + Dead Letter Topic | 3 retries, 1s apart; poison messages routed to `<topic>.DLT`. Safe because consumers are idempotent |
| HTTP idempotency | `idempotencyKey` unique constraint | Duplicate POST requests return the existing order instead of creating a new one |

## Project Structure

```
src/main/java/com/example/demo/
├── DemoApplication.java
├── config/
│   └── KafkaConfig.java              # Retry + DLT configuration
├── controller/
│   ├── OrderController.java          # REST endpoints
│   └── GlobalExceptionHandler.java   # Validation + state error handling
├── dto/
│   ├── OrderRequest.java             # Input with Bean Validation
│   ├── OrderItemRequest.java
│   └── OrderResponse.java
├── event/
│   ├── OrderEvent.java               # Kafka event payloads
│   └── PaymentEvent.java
├── model/
│   ├── Order.java                    # JPA entity with optimistic locking
│   ├── OrderItem.java
│   ├── OrderStatus.java              # State machine enum
│   ├── OutboxEvent.java              # Transactional outbox entity
│   └── ProcessedEvent.java           # Idempotency tracking
├── repository/
│   ├── OrderRepository.java
│   ├── OutboxRepository.java
│   └── ProcessedEventRepository.java
└── service/
    ├── OrderService.java             # Core order logic + outbox write
    ├── OutboxPublisher.java          # Scheduled poller -> Kafka
    ├── PaymentService.java           # Kafka consumer, simulates payment
    ├── OrderStatusUpdater.java       # Payment result -> order status
    └── NotificationService.java      # Event logger
```

## License

MIT
