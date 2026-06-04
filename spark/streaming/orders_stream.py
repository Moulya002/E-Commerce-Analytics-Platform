"""
Spark Structured Streaming: Orders pipeline
- Consumes orders Kafka topic
- Computes revenue per minute, geographic metrics, top products
- Writes bronze (S3 parquet) and gold metrics (PostgreSQL)
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from configs.logging_config import setup_logging
from configs.settings import settings
from spark.common.spark_session import create_spark_session

logger = setup_logging(__name__)

ORDER_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

PG_PROPS = {
    "user": settings.postgres.user,
    "password": settings.postgres.password,
    "driver": "org.postgresql.Driver",
}


def read_kafka_stream(spark, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-orders-consumer")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_orders(kafka_df):
    return (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )


def _write_to_postgres(batch_df, table: str, batch_id: int) -> None:
    if batch_df.count() == 0:
        return
    (
        batch_df.write
        .format("jdbc")
        .option("url", settings.postgres.metrics_jdbc_url)
        .option("dbtable", table)
        .mode("append")
        .options(**PG_PROPS)
        .save()
    )
    logger.info("Wrote %s batch_id=%d", table, batch_id)


def write_bronze_lake(df, checkpoint: str):
    bronze_path = f"{settings.s3.bronze_path}orders/"
    return (
        df.writeStream
        .format("parquet")
        .option("path", bronze_path)
        .option("checkpointLocation", f"{checkpoint}/bronze_orders")
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )


def revenue_per_minute(df, checkpoint: str):
    windowed = (
        df
        .withWatermark("event_time", settings.spark.watermark_delay)
        .groupBy(F.window("event_time", "1 minute").alias("time_window"))
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "total_revenue",
            "order_count",
            F.current_timestamp().alias("updated_at"),
        )
    )

    return (
        windowed.writeStream
        .foreachBatch(lambda b, i: _write_to_postgres(b, "realtime.revenue_per_minute", i))
        .option("checkpointLocation", f"{checkpoint}/revenue_per_minute")
        .outputMode("update")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )


def geographic_metrics(df, checkpoint: str):
    geo_df = (
        df
        .withWatermark("event_time", settings.spark.watermark_delay)
        .groupBy(
            F.window("event_time", "1 minute").alias("time_window"),
            F.col("country"),
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            "country",
            "order_count",
            "revenue",
            F.current_timestamp().alias("updated_at"),
        )
    )

    return (
        geo_df.writeStream
        .foreachBatch(lambda b, i: _write_to_postgres(b, "realtime.geographic_metrics", i))
        .option("checkpointLocation", f"{checkpoint}/geo_metrics")
        .outputMode("update")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )


def top_products(df, checkpoint: str):
    aggregated = (
        df
        .withWatermark("event_time", settings.spark.watermark_delay)
        .groupBy(
            F.window("event_time", "1 minute").alias("time_window"),
            F.col("product_id"),
            F.first("product_name").alias("product_name"),
        )
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
        )
    )

    window_spec = Window.partitionBy("time_window").orderBy(F.desc("revenue"))
    ranked = (
        aggregated
        .withColumn("rank_position", F.row_number().over(window_spec))
        .filter(F.col("rank_position") <= 5)
        .select(
            F.col("time_window.start").alias("window_start"),
            "product_id",
            "product_name",
            "units_sold",
            "revenue",
            "rank_position",
            F.current_timestamp().alias("updated_at"),
        )
    )

    return (
        ranked.writeStream
        .foreachBatch(lambda b, i: _write_to_postgres(b, "realtime.top_products", i))
        .option("checkpointLocation", f"{checkpoint}/top_products")
        .outputMode("complete")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )


def main():
    spark = create_spark_session("orders-streaming")
    checkpoint = f"{settings.spark.checkpoint_base}/orders"

    orders_df = parse_orders(read_kafka_stream(spark, settings.kafka.orders_topic))

    logger.info("Starting orders streaming pipelines...")
    queries = [
        write_bronze_lake(orders_df, checkpoint),
        revenue_per_minute(orders_df, checkpoint),
        geographic_metrics(orders_df, checkpoint),
        top_products(orders_df, checkpoint),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
