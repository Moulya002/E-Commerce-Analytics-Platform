"""
Airflow DAG: CDC pipeline health monitoring
Validates Debezium connector status and simulates DB changes
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class DebeziumHealthSensor(BaseSensorOperator):
    """Check Kafka Connect / Debezium connector health."""

    def poke(self, context: Context) -> bool:
        import urllib.request
        import json

        try:
            url = "http://kafka-connect:8083/connectors/ecommerce-postgres-connector/status"
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                return data.get("connector", {}).get("state") == "RUNNING"
        except Exception:
            return False


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ecommerce_cdc_monitoring",
    default_args=default_args,
    description="Monitor CDC pipeline health and trigger test changes",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "cdc", "debezium"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_connector = DebeziumHealthSensor(
        task_id="check_debezium_connector",
        poke_interval=30,
        timeout=120,
    )

    simulate_changes = BashOperator(
        task_id="simulate_cdc_changes",
        bash_command="python /opt/airflow/cdc/scripts/simulate_cdc_changes.py || echo 'CDC simulator skipped'",
    )

    verify_cdc_topics = BashOperator(
        task_id="verify_cdc_kafka_topics",
        bash_command="""
        kafka-console-consumer \
          --bootstrap-server kafka:29092 \
          --topic ecommerce_cdc.ecommerce_cdc.orders \
          --from-beginning \
          --max-messages 1 \
          --timeout-ms 10000 || echo "No CDC messages yet"
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> check_connector >> simulate_changes >> verify_cdc_topics >> end
