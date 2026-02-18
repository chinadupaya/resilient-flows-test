# Saga Pattern with CDC and Workflow Engines

This repository contains two Saga pattern implementations designed for experimenting with Debezium for data reconciliation between microservices and Restate to compliment resiliency for durable transactions.

## Project Goal - CDC first
Experiment with how CDC on top of microservice databases helps with data reconciliation without using the outbox pattern. This setup intentionally accepts the volatility of direct Kafka publishing (async) or REST calls (sync) to observe how CDC can detect and help reconcile inconsistencies.

## Two Communication Methods
1. Asynchronous Communication (Kafka-based)

- Communication: Events published directly to Kafka
- Risk: DB commit succeeds but Kafka publish fails → lost events
- CDC Solution: Captures account/transaction table changes as fallback

2. Synchronous Communication (REST-based)

- Communication: Direct HTTP REST calls between services
- Risk: Partial failures leave data inconsistent
- CDC Solution: Tracks state changes across both databases

We will be using the same API call `POST /transactions` but create an additional argument if they are sync or async `type=sync`or `type=async`

## Failure Scenarios
- One failure each for async and sync processes