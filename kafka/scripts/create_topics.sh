#!/bin/bash
# Core Kafka topics for e-commerce event streaming (local portfolio)

set -e
BOOTSTRAP="kafka:29092"

create_topic() {
  echo "Creating topic: $1 (partitions=$2)"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=lz4
}

# Event streams (partitioned by business key via producer)
create_topic "orders" 6
create_topic "payments" 6
create_topic "clicks" 12
create_topic "users" 3
create_topic "inventory_updates" 3

echo "Core topics ready:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
