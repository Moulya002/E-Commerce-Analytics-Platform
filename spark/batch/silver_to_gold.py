"""
Local batch ETL (simulates AWS Glue job).
Reads bronze parquet from local data lake → writes gold aggregates.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import functions as F

from configs.logging_config import setup_logging
from configs.settings import settings
from spark.common.spark_session import create_spark_session

logger = setup_logging(__name__)


def main():
    spark = create_spark_session("local-silver-to-gold")
    bronze_orders = f"{settings.data_lake.bronze_path}orders/"
    gold_path = f"{settings.data_lake.gold_path}daily_revenue/"

    logger.info("Reading bronze from %s", bronze_orders)
    try:
        orders_df = spark.read.parquet(bronze_orders)
    except Exception as exc:
        logger.warning("No bronze orders yet: %s", exc)
        spark.stop()
        return

    gold_df = (
        orders_df
        .withColumn("order_date", F.to_date("event_time"))
        .groupBy("order_date", "country", "category")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    gold_df.write.mode("overwrite").partitionBy("order_date").parquet(gold_path)
    logger.info("Gold layer written to %s", gold_path)
    spark.stop()


if __name__ == "__main__":
    main()
