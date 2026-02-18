#!/bin/bash

HOST="http://spanner-emulator:9020"
PROJECT="test-project"
INSTANCE="test-instance"
DATABASE="test-database"

echo "Waiting for emulator..."
until curl -s "$HOST" > /dev/null 2>&1; do
  sleep 1
done

echo "Creating instance..."
curl -s -X POST "$HOST/v1/projects/$PROJECT/instances" \
  -H "Content-Type: application/json" \
  -d '{
    "instanceId": "test-instance",
    "instance": {
      "config": "projects/test-project/instanceConfigs/emulator-config",
      "displayName": "Test Instance",
      "nodeCount": 1
    }
  }'

echo "Creating database..."
curl -s -X POST "$HOST/v1/projects/$PROJECT/instances/$INSTANCE/databases" \
  -H "Content-Type: application/json" \
  -d '{
    "createStatement": "CREATE DATABASE `test-database`",
    "extraStatements": [
      "CREATE TABLE transactions (id STRING(36) NOT NULL, source_account_id STRING(36) NOT NULL, destination_account_id STRING(36) NOT NULL, amount NUMERIC NOT NULL, status STRING(20) NOT NULL, saga_state STRING(50) NOT NULL, failure_reason STRING(500), created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP, version INT64 NOT NULL) PRIMARY KEY (id)"
    ]
  }'

echo "Done!"