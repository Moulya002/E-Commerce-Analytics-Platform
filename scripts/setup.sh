#!/bin/bash
# Core local setup: Kafka + Postgres + Spark Streaming + Dashboard
# Optional: docker compose --profile cdc --profile airflow up -d

set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

echo "=== E-Commerce Data Platform — Core Setup ==="

if ! command -v docker >/dev/null 2>&1; then
  for _docker_bin in \
    "/Applications/Docker.app/Contents/Resources/bin" \
    "/usr/local/bin" \
    "$HOME/.docker/bin"; do
    if [ -x "${_docker_bin}/docker" ]; then
      export PATH="${_docker_bin}:$PATH"
      break
    fi
  done
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: Install Docker Desktop → https://www.docker.com/products/docker-desktop/"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Start Docker Desktop, then re-run this script."
  exit 1
fi

[ -f .env ] || cp .env.example .env

mkdir -p data/lake/bronze data/lake/silver data/lake/gold data/lake/checkpoints
chmod -R 777 data/lake/checkpoints 2>/dev/null || true
chmod +x kafka/scripts/create_topics.sh cdc/scripts/register_connector.sh scripts/*.sh 2>/dev/null || true

echo "Building images (producer, dashboard, spark-streaming)..."
docker compose build producer dashboard spark-streaming

echo "Starting core pipeline..."
docker compose up -d zookeeper kafka postgres spark-master spark-worker
echo "Waiting for Kafka (20s)..."
sleep 20
docker compose up kafka-init
docker compose up -d producer dashboard spark-streaming

echo ""
echo "=== Core pipeline is starting ==="
echo ""
echo "  Dashboard:     http://localhost:8501"
echo "  Spark UI:      http://localhost:8080"
echo "  Kafka:         localhost:9092"
echo "  PostgreSQL:    localhost:5432  (ecommerce_user / ecommerce_pass)"
echo ""
echo "Metrics populate after ~1–2 min (Spark trigger: 30s windows)."
echo "  Health check:  ./scripts/healthcheck.sh"
echo "  Seed demo:     python3 scripts/seed_metrics.py"
echo ""
echo "Optional profiles:"
echo "  CDC:      docker compose --profile cdc up -d"
echo "  Airflow:  docker compose --profile airflow up -d"
echo ""
