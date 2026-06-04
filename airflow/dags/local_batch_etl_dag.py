"""
Local batch ETL DAG — no AWS dependencies.
Simulates Glue + warehouse load using Spark batch + PostgreSQL.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="local_batch_etl",
    description="Bronze → Gold parquet + warehouse summary (local portfolio)",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["local", "batch", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command="docker exec spark-master /opt/bitnami/spark/bin/spark-submit "
        "--master spark://spark-master:7077 /workspace/spark/batch/silver_to_gold.py",
    )

    warehouse_load = BashOperator(
        task_id="load_warehouse_summary",
        bash_command="docker exec -i postgres psql -U ecommerce_user -d ecommerce -c "
        "\"INSERT INTO warehouse.daily_revenue_summary SELECT CURRENT_DATE, "
        "COALESCE(SUM(total_revenue),0), COALESCE(SUM(order_count),0), "
        "COALESCE(AVG(total_revenue/NULLIF(order_count,0)),0), 1, NOW() "
        "FROM realtime.revenue_per_minute WHERE window_start >= NOW() - INTERVAL '1 day' "
        "ON CONFLICT (report_date) DO UPDATE SET total_revenue=EXCLUDED.total_revenue;\"",
    )

    end = EmptyOperator(task_id="end")
    start >> silver_to_gold >> warehouse_load >> end
