"""
Spark Structured Streaming: CDC pipeline
Consumes Debezium change events from PostgreSQL and writes to silver layer.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from configs.logging_config import setup_logging
from configs.settings import settings
from spark.common.spark_session import create_spark_session

logger = setup_logging(__name__)

CDC_SCHEMA = StructType([
    StructField("op", StringType(), True),
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
    StructField("source", StructType([
        StructField("table", StringType(), True),
        StructField("db", StringType(), True),
    ])),
])


def main():
    spark = create_spark_session("cdc-streaming")
    checkpoint = f"{settings.spark.checkpoint_base}/cdc"

    cdc_topics = [
        "ecommerce_cdc.ecommerce_cdc.customers",
        "ecommerce_cdc.ecommerce_cdc.products",
        "ecommerce_cdc.ecommerce_cdc.orders",
        "ecommerce_cdc.ecommerce_cdc.inventory_log",
    ]

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", ",".join(cdc_topics))
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "ecommerce-spark-cdc-v1")
        .load()
    )

    cdc_df = (
        kafka_df
        .select(
            F.col("topic").alias("cdc_topic"),
            F.col("key").cast("string").alias("record_key"),
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("year", F.year("processed_at"))
        .withColumn("month", F.month("processed_at"))
        .withColumn("day", F.dayofmonth("processed_at"))
        .withColumn(
            "table_name",
            F.regexp_extract("cdc_topic", r"\.([^.]+)$", 1),
        )
        .withColumn(
            "operation",
            F.get_json_object("payload", "$.op"),
        )
    )

    silver_path = f"{settings.s3.silver_path}cdc/"

    (
        cdc_df.writeStream
        .format("parquet")
        .option("path", silver_path)
        .option("checkpointLocation", f"{checkpoint}/silver_cdc")
        .partitionBy("table_name", "year", "month", "day")
        .outputMode("append")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    logger.info("CDC streaming pipeline started | silver_path=%s", silver_path)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
