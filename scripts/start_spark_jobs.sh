#!/bin/bash
# Restart the unified Spark Structured Streaming job
set -e
cd "$(dirname "$0")/.."
echo "Restarting spark-streaming container..."
docker compose restart spark-streaming
echo "Monitor: docker logs -f spark-streaming"
echo "Spark UI: http://localhost:8080"
