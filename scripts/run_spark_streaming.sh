#!/bin/bash
set -e
export HOME="${HOME:-/tmp}"
export IVY_HOME="${IVY_HOME:-/tmp/.ivy2}"
mkdir -p "${IVY_HOME}/local" "${IVY_HOME}/cache"

mkdir -p /data-lake/checkpoints
chmod -R 777 /data-lake/checkpoints 2>/dev/null || true

echo "Waiting for Spark cluster..."
sleep 30

exec /opt/bitnami/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /workspace/spark/streaming/unified_streaming.py
