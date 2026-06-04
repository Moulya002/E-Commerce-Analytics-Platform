#!/bin/bash
set -e
cd "$(dirname "$0")/.."
echo "Stopping all services (including optional profiles)..."
docker compose --profile cdc --profile airflow --profile s3 down -v --remove-orphans
echo "Teardown complete."
