#!/bin/bash
# Local batch ETL (simulates AWS Glue silver→gold + warehouse load)

set -e
cd "$(dirname "$0")/.."

echo "Running local batch ETL (Spark silver→gold)..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /workspace/spark/batch/silver_to_gold.py 2>/dev/null || \
docker compose run --rm -e PYTHONPATH=/workspace spark-streaming \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/spark/batch/silver_to_gold.py

echo "Refreshing warehouse summary from realtime metrics..."
docker exec -i postgres psql -U ecommerce_user -d ecommerce <<'SQL'
INSERT INTO warehouse.daily_revenue_summary (report_date, total_revenue, order_count, avg_order_value, unique_countries)
SELECT
    CURRENT_DATE,
    COALESCE(SUM(total_revenue), 0),
    COALESCE(SUM(order_count), 0),
    COALESCE(AVG(total_revenue / NULLIF(order_count, 0)), 0),
    (SELECT COUNT(DISTINCT country) FROM realtime.geographic_metrics WHERE window_start >= NOW() - INTERVAL '1 day')
FROM realtime.revenue_per_minute
WHERE window_start >= NOW() - INTERVAL '1 day'
ON CONFLICT (report_date) DO UPDATE SET
    total_revenue = EXCLUDED.total_revenue,
    order_count = EXCLUDED.order_count,
    avg_order_value = EXCLUDED.avg_order_value,
    etl_loaded_at = NOW();
SQL

echo "Batch ETL complete. Gold data: ./data/lake/gold/"
