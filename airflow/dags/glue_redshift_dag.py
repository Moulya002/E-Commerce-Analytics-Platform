"""
Airflow DAG: Trigger AWS Glue jobs and load Redshift warehouse
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftDataOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ecommerce_glue_redshift_load",
    default_args=default_args,
    description="Glue ETL + Redshift warehouse load pipeline",
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "glue", "redshift", "warehouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    crawl_bronze = GlueCrawlerOperator(
        task_id="crawl_bronze_layer",
        config={"Name": "ecommerce_bronze_crawler"},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    run_glue_etl = GlueJobOperator(
        task_id="run_silver_to_gold_glue_etl",
        job_name="ecommerce-silver-to-gold-etl",
        script_args={
            "--SOURCE_PATH": "s3://ecommerce-data-lake/silver/",
            "--TARGET_PATH": "s3://ecommerce-data-lake/gold/",
            "--DATABASE": "ecommerce_analytics",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    crawl_gold = GlueCrawlerOperator(
        task_id="crawl_gold_layer",
        config={"Name": "ecommerce_gold_crawler"},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    load_redshift = RedshiftDataOperator(
        task_id="load_redshift_fact_orders",
        sql="/opt/airflow/redshift/load/copy_from_s3.sql",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    run_redshift_analyze = RedshiftDataOperator(
        task_id="analyze_redshift_tables",
        sql="ANALYZE ecommerce_dw.fact_orders; ANALYZE ecommerce_dw.fact_payments;",
        aws_conn_id="aws_default",
    )

    end = EmptyOperator(task_id="end")

    start >> crawl_bronze >> run_glue_etl >> crawl_gold >> load_redshift >> run_redshift_analyze >> end
