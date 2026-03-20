# Saga Pattern with CDC and Workflow Engines

This repository contains two Saga pattern implementations designed for experimenting with Debezium for data reconciliation between microservices and Restate to compliment resiliency for durable transactions.

## Project Goal
Experiment with how CDC + Durable Engines layers on top of heterogeneous microservices helps with data reconciliation and resiliency. This setup intentionally accepts the volatility of direct Kafka publishing (async) or REST calls (sync) to observe how these 2 layers can detect and help reconcile inconsistencies.

## Two Communication Methods
1. Asynchronous Communication (Kafka-based)

- Communication: Events published directly to Kafka

2. Synchronous Communication (REST-based)

- Communication: Direct HTTP REST calls between services

We will be using the same API call `POST /transactions` but create an additional argument if they are sync or async `type=sync`or `type=async`

## Failure Scenarios
- One failure each for async and sync processes

# Ports Overview (localhost)
- Account service 7070
- Transaction service 9090
  - Restate connection port 9091
- Restate

# To Run locally:

## Requirements
- Docker, Docker compose
- Java

## Running
1. Go to `/infrastructure` and run `docker compose up -d`
  - Initialization for transaction and account service is done automatically.
2. Create the connectors from the same folder
```
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @postgres-connector.json
```
Optionally, verify if messages are being received
```
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic account.public.accounts \
  --from-beginning
```

4. Run account service and transaction service from their directories.
```
mvn spring-boot:run
```

5. Connect to Restate

### Other important commands:

See all topics:
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list