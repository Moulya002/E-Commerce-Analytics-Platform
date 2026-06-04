#!/bin/bash
# Submit all Spark Structured Streaming jobs to the cluster

set -e

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
SPARK_APPS="/opt/spark-apps"
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

submit_job() {
  local name=$1
  local script=$2
  echo "Submitting: ${name}"
  spark-submit \
    --master "${SPARK_MASTER}" \
    --packages "${PACKAGES}" \
    --deploy-mode client \
    --name "${name}" \
    "${SPARK_APPS}/streaming/${script}"
}

submit_job "orders-stream" "orders_stream.py"
submit_job "payments-stream" "payments_stream.py"
submit_job "clicks-stream" "clicks_stream.py"
submit_job "users-stream" "users_stream.py"
submit_job "cdc-stream" "cdc_stream.py"

echo "All streaming jobs submitted."
