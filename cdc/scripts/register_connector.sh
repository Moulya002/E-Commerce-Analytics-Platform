#!/bin/sh
# Register Debezium PostgreSQL connector with Kafka Connect

set -e

CONNECT_URL="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_FILE="/connectors/postgres-connector.json"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
i=0
while [ $i -lt $MAX_RETRIES ]; do
  if curl -sf "${CONNECT_URL}/" > /dev/null 2>&1; then
    echo "Kafka Connect is ready."
    break
  fi
  i=$((i + 1))
  echo "Attempt $i/$MAX_RETRIES - retrying in ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
done

if [ $i -eq $MAX_RETRIES ]; then
  echo "ERROR: Kafka Connect not available"
  exit 1
fi

# Delete existing connector if present (idempotent setup)
curl -sf -X DELETE "${CONNECT_URL}/connectors/ecommerce-postgres-connector" 2>/dev/null || true
sleep 2

echo "Registering Debezium connector..."
HTTP_CODE=$(curl -s -o /tmp/connector_response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  --data @"${CONNECTOR_FILE}" \
  "${CONNECT_URL}/connectors")

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  echo "Connector registered successfully."
  curl -s "${CONNECT_URL}/connectors/ecommerce-postgres-connector/status" | head -c 2000
else
  echo "ERROR: Failed to register connector (HTTP $HTTP_CODE)"
  cat /tmp/connector_response.json
  exit 1
fi
