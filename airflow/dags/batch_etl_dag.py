"""
Airflow DAG: Daily batch ETL pipeline
Silver → Gold transformation and metrics aggregation
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_batch_etl",
    default_args=default_args,
    description="Daily batch ETL: silver to gold layer transformation",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "batch", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_silver_to_gold = BashOperator(
        task_id="run_spark_silver_to_gold",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /opt/spark-apps/batch/silver_to_gold.py
        """,
    )

    refresh_daily_summary = PostgresOperator(
        task_id="refresh_daily_revenue_summary",
        postgres_conn_id="postgres_metrics",
        sql="""
        INSERT INTO batch.daily_revenue_summary (report_date, total_revenue, order_count, avg_order_value)
        SELECT
            DATE(window_start) AS report_date,
            SUM(total_revenue),
            SUM(order_count),
            AVG(total_revenue / NULLIF(order_count, 0))
        FROM realtime.revenue_per_minute
        WHERE DATE(window_start) = CURRENT_DATE - 1
        GROUP BY DATE(window_start)
        ON CONFLICT (report_date) DO UPDATE SET
            total_revenue = EXCLUDED.total_revenue,
            order_count = EXCLUDED.order_count,
            avg_order_value = EXCLUDED.avg_order_value;
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> run_silver_to_gold >> refresh_daily_summary >> end
