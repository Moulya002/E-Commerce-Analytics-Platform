"""
Airflow DAG: Spark Structured Streaming job management
Ensures streaming jobs are running (health check + restart)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

STREAMING_JOBS = [
    ("orders", "orders_stream.py"),
    ("payments", "payments_stream.py"),
    ("clicks", "clicks_stream.py"),
    ("users", "users_stream.py"),
    ("cdc", "cdc_stream.py"),
]

with DAG(
    dag_id="ecommerce_spark_streaming",
    default_args=default_args,
    description="Manage Spark Structured Streaming jobs",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "streaming", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    prev_task = start
    for job_name, script in STREAMING_JOBS:
        submit_task = BashOperator(
            task_id=f"submit_{job_name}_stream",
            bash_command=f"""
            # Check if job is already running; submit if not
            RUNNING=$(ps aux | grep -c "{script}" || true)
            if [ "$RUNNING" -lt 2 ]; then
              spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
                /opt/spark-apps/streaming/{script}
            else
              echo "Job {job_name} already running"
            fi
            """,
        )
        prev_task >> submit_task
        prev_task = submit_task

    prev_task >> end
