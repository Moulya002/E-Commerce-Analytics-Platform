#!/bin/bash
# Verify core services are healthy

set -e
cd "$(dirname "$0")/.."

echo "=== Platform Health Check ==="
fail=0

check() {
  if eval "$2" >/dev/null 2>&1; then
    echo "  OK   $1"
  else
    echo "  FAIL $1"
    fail=1
  fi
}

check "Docker daemon" "docker info"
check "Kafka container" "docker ps --format '{{.Names}}' | grep -q '^kafka$'"
check "Postgres container" "docker ps --format '{{.Names}}' | grep -q '^postgres$'"
check "Producer container" "docker ps --format '{{.Names}}' | grep -q '^event-producer$'"
check "Spark streaming" "docker ps --format '{{.Names}}' | grep -q '^spark-streaming$'"
check "Dashboard container" "docker ps --format '{{.Names}}' | grep -q '^dashboard$'"
check "Dashboard HTTP" "curl -sf http://localhost:8501/_stcore/health"
check "Spark UI HTTP" "curl -sf http://localhost:8080"

echo ""
echo "Kafka topics:"
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null || echo "  (kafka not ready)"

echo ""
echo "Recent producer logs:"
docker logs event-producer --tail 3 2>/dev/null || true

echo ""
echo "Recent spark-streaming logs:"
docker logs spark-streaming --tail 5 2>/dev/null || true

if [ -d data/lake/bronze/orders ]; then
  echo ""
  echo "Bronze parquet files:"
  find data/lake/bronze -name "*.parquet" 2>/dev/null | head -5 || echo "  (none yet — wait for Spark)"
fi

echo ""
if [ "$fail" -eq 0 ]; then
  echo "All core checks passed."
else
  echo "Some checks failed. Run: docker compose ps && docker compose logs spark-streaming"
  exit 1
fi
