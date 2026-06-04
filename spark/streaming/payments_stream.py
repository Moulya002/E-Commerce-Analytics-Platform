"""
Spark Structured Streaming: Payments pipeline
- Tracks failed payments and payment volume per minute
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from configs.logging_config import setup_logging
from configs.settings import settings
from spark.common.spark_session import create_spark_session

logger = setup_logging(__name__)

PAYMENT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True),
])

PG_PROPS = {
    "user": settings.postgres.user,
    "password": settings.postgres.password,
    "driver": "org.postgresql.Driver",
}


def main():
    spark = create_spark_session("payments-streaming")
    checkpoint = f"{settings.spark.checkpoint_base}/payments"

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.payments_topic)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-payments-consumer")
        .load()
    )

    payments_df = (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), PAYMENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )

    # Bronze layer
    bronze_query = (
        payments_df.writeStream
        .format("parquet")
        .option("path", f"{settings.s3.bronze_path}payments/")
        .option("checkpointLocation", f"{checkpoint}/bronze")
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    # Failed payments per minute
    failed_df = (
        payments_df
        .filter(F.col("status") == "failed")
        .withWatermark("event_time", settings.spark.watermark_delay)
        .groupBy(F.window("event_time", "1 minute").alias("time_window"))
        .agg(
            F.count("payment_id").alias("failure_count"),
            F.sum("amount").alias("failure_amount"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "failure_count",
            "failure_amount",
            F.current_timestamp().alias("updated_at"),
        )
    )

    def write_failed(batch_df, batch_id):
        if batch_df.count() > 0:
            (
                batch_df.write
                .format("jdbc")
                .option("url", settings.postgres.metrics_jdbc_url)
                .option("dbtable", "realtime.failed_payments")
                .mode("append")
                .options(**PG_PROPS)
                .save()
            )
            logger.info("Wrote failed_payments batch_id=%d", batch_id)

    metrics_query = (
        failed_df.writeStream
        .foreachBatch(write_failed)
        .option("checkpointLocation", f"{checkpoint}/failed_payments")
        .outputMode("update")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
