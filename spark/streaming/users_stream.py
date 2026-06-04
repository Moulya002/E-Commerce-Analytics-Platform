"""
Spark Structured Streaming: Active users pipeline
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

USER_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("activity_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("activity_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    StructField("timestamp", StringType(), True),
])

PG_PROPS = {
    "user": settings.postgres.user,
    "password": settings.postgres.password,
    "driver": "org.postgresql.Driver",
}


def main():
    spark = create_spark_session("users-streaming")
    checkpoint = f"{settings.spark.checkpoint_base}/users"

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.users_topic)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-users-consumer")
        .load()
    )

    users_df = (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), USER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )

    (
        users_df.writeStream
        .format("parquet")
        .option("path", f"{settings.s3.bronze_path}users/")
        .option("checkpointLocation", f"{checkpoint}/bronze")
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    active_users_df = (
        users_df
        .withWatermark("event_time", settings.spark.watermark_delay)
        .groupBy(F.window("event_time", "1 minute").alias("time_window"))
        .agg(F.approx_count_distinct("user_id").alias("active_user_count"))
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "active_user_count",
            F.current_timestamp().alias("updated_at"),
        )
    )

    def write_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            (
                batch_df.write
                .format("jdbc")
                .option("url", settings.postgres.metrics_jdbc_url)
                .option("dbtable", "realtime.active_users")
                .mode("append")
                .options(**PG_PROPS)
                .save()
            )
            logger.info("Wrote active_users batch_id=%d", batch_id)

    (
        active_users_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{checkpoint}/active_users")
        .outputMode("update")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
