# Saga Orchestration — Java 21 / Spring Boot 3 / Kafka / PostgreSQL

## Architecture

The system consists of four microservices:

- **order-service (8081)**: Acts as the Saga orchestrator.
- **payment-service (8082)**: Handles payment authorization and compensation.
- **inventory-service (8083)**: Manages inventory reservations and releases.
- **shipping-service (8084)**: Handles shipment creation and cancellation.

Each service performs local transactions against its own PostgreSQL database.

Kafka is used to publish notifications about Saga execution results.

Orchestration commands are executed through **synchronous HTTP calls**. This implementation is **not an asynchronous
Kafka-based orchestrator**.

## Requirements

- JDK 21
- Maven 3.9+
- Docker Desktop with Docker Compose

## Getting Started

From the project root directory:

```bash
mvn clean package -DskipTests
docker compose up --build -d
docker compose ps
```

The services may take a few seconds to start.

Before running the tests, verify that the following endpoint returns a status of `UP`:

```text
http://localhost:8081/actuator/health
```

## Testing (PowerShell)

```powershell
# Successful Saga execution
$ok = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method Post -ContentType 'application/json' -Body '{"failAt":"none"}'
$ok | ConvertTo-Json

# Inventory failure: void payment authorization
$bad = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method Post -ContentType 'application/json' -Body '{"failAt":"inventory"}'
$bad | ConvertTo-Json

# Shipping failure: release inventory and void payment
$shipping = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method Post -ContentType 'application/json' -Body '{"failAt":"shipping"}'
$shipping | ConvertTo-Json

# Verify compensated states
Invoke-RestMethod "http://localhost:8082/internal/payment/$($bad.id)"
Invoke-RestMethod "http://localhost:8083/internal/inventory/$($shipping.id)"
```

## Saga Execution Flow

1. `POST /orders` creates an order with the initial status `PENDING`.
2. The orchestrator sequentially invokes `payment/authorize`, `inventory/reserve`, and `shipping/create`.
3. If any step fails, the orchestrator executes compensating transactions in reverse order: `shipping/cancel`,
   `inventory/release`, and `payment/void`, but only for steps that have already completed successfully.
4. The final order status is `COMPLETED`, `CANCELLED`, or `COMPENSATION_FAILED`.
5. A best-effort notification is published to the Kafka topic `saga-order-events`.

## Idempotency and Educational Limitations

- **Idempotency:** Each service uses `orderId` as its primary key. Repeating a successful request is idempotent, and
  repeating a compensation operation produces no additional side effects.

- **Simulated payment processing:** The payment `void` operation simulates the cancellation of a payment authorization.
  No real payment gateway is contacted. Inventory and shipping maintain persistent states but do not integrate with
  actual external systems.

- **Synchronous execution:** The orchestration flow is synchronous, and Saga execution does not automatically resume
  after an orchestrator crash. A request timeout does not necessarily indicate that the remote operation failed. State
  reconciliation is required before executing compensating transactions.

- **Kafka publication consistency:** Kafka event publication is **not atomic** with the database transaction and is not
  guaranteed. A production-ready implementation should include the Transactional Outbox Pattern with an event relay
  (such as Debezium), retries, Dead Letter Queues (DLQs), correlation IDs, and idempotent consumers.

- **Production limitations:** Authentication, TLS, distributed tracing, explicit timeout and retry policies, circuit
  breakers, concurrency locking, Testcontainers integration tests, and a persistent Saga process manager are not
  included. This demonstration must not be deployed to production without additional architectural and security
  improvements.

- **Internal API security:** The `/internal/**` endpoints are publicly exposed for educational purposes. In production
  environments, these endpoints must be private and protected by appropriate authentication and authorization
  mechanisms.

## System Design Exercises

1. **Event-driven orchestration:** Replace synchronous HTTP commands with Kafka commands and events, introducing a
   persistent state-machine-based Saga orchestrator.

2. **Transactional Outbox and CDC:** Implement the Transactional Outbox Pattern and Change Data Capture (CDC) using
   Debezium to ensure reliable event publication.

3. **Failure recovery:** Simulate orchestrator timeouts and crashes, then implement automatic Saga recovery after
   application restart.

4. **Idempotency and resilience:** Introduce client-side idempotency keys, optimistic locking, exponential backoff
   retries, and Dead Letter Queues (DLQs).

5. **Integration testing and observability:** Add Testcontainers for PostgreSQL and Kafka, Micrometer metrics, and
   OpenTelemetry distributed tracing.

## Useful Commands

```bash
# Follow order-service logs
docker compose logs -f order-service

# Stop and remove containers
docker compose down

# Stop containers and delete database volumes
# WARNING: This permanently removes persisted database data.
docker compose down -v
```