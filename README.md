# Saga Orchestration — Java 21 / Spring Boot 3 / Kafka / PostgreSQL

## Architettura
Quattro microservizi: **order-service (8081)** è l'orchestratore; **payment-service (8082)**, **inventory-service (8083)** e **shipping-service (8084)** eseguono transazioni locali su database PostgreSQL separati. Kafka riceve notifiche del risultato della Saga. I comandi di orchestrazione sono HTTP sincroni: **non** si tratta di un orchestratore Kafka asincrono.

## Requisiti
JDK 21, Maven 3.9+, Docker Desktop + Docker Compose.

## Avvio
Dalla directory radice:

```bash
mvn clean package -DskipTests
docker compose up --build -d
docker compose ps
```

I servizi richiedono qualche secondo per avviarsi; attendere `http://localhost:8081/actuator/health` con stato `UP` prima delle prove.

## Prove (PowerShell)

```powershell
# Successo
$ok = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method Post -ContentType 'application/json' -Body '{"failAt":"none"}'
$ok | ConvertTo-Json
# Errore magazzino: annulla autorizzazione pagamento
$bad = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method Post -ContentType 'application/json' -Body '{"failAt":"inventory"}'
$bad | ConvertTo-Json
# Errore spedizione: rilascia inventario e annulla pagamento
$shipping = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method Post -ContentType 'application/json' -Body '{"failAt":"shipping"}'
$shipping | ConvertTo-Json
# Verifica dello stato compensato
Invoke-RestMethod "http://localhost:8082/internal/payment/$($bad.id)"
Invoke-RestMethod "http://localhost:8083/internal/inventory/$($shipping.id)"
```

## Flusso
1. `POST /orders` crea ordine `PENDING`.
2. L'orchestratore invoca `payment/authorize`, `inventory/reserve`, `shipping/create`.
3. Se un passo fallisce, invoca le compensazioni in ordine inverso: `shipping/cancel`, `inventory/release`, `payment/void` (solo sui passi già completati).
4. Stato finale `COMPLETED`, `CANCELLED` oppure `COMPENSATION_FAILED`.
5. Notifica best-effort su topic Kafka `saga-order-events`.

## Idempotenza e limiti didattici
- Ogni servizio usa `orderId` come chiave primaria: ripetere una richiesta di successo è idempotente; ripetere una compensazione non ha effetti aggiuntivi.
- `void` del pagamento è un annullamento **simulato** di autorizzazione: non viene contattato un gateway di pagamento reale. Inventory e shipping usano stati persistiti, ma non integrano sistemi esterni reali.
- Il flusso è sincrono e l'ordine non viene ripreso automaticamente dopo crash. Una richiesta che va in timeout può aver completato il passo remoto; occorre riconciliazione dello stato prima di compensare.
- La pubblicazione Kafka **non è atomica** con la transazione DB e non è garantita; in produzione aggiungere Transactional Outbox + relay (per esempio Debezium), retry, DLQ, correlation ID e consumer idempotenti.
- Non sono inclusi autenticazione, TLS, tracing distribuito, timeout/retry espliciti, circuit breaker, lock di concorrenza, test di integrazione Testcontainers e process manager persistente. Non usare questo demo così com'è in produzione.
- Le API `/internal/**` sono esposte per esercizio; in produzione devono essere private e autenticate.

## Esercizi System Design
1. Sostituire i comandi HTTP con comandi/eventi Kafka e un orchestratore a macchina a stati persistente.
2. Aggiungere outbox transazionale e CDC con Debezium.
3. Simulare timeout e crash dell'orchestratore e implementare recovery al riavvio.
4. Gestire idempotency key lato client, optimistic locking, retry esponenziali e dead-letter queue.
5. Aggiungere Testcontainers PostgreSQL/Kafka, metriche Micrometer e tracing OpenTelemetry.

## Comandi utili
```bash
docker compose logs -f order-service
docker compose down
docker compose down -v  # elimina i database: ATTENZIONE
```
