"""
Unified Spark Structured Streaming pipeline (portfolio / local demo).

Consumes Kafka → bronze parquet (local data lake) → PostgreSQL realtime metrics.
Uses watermarking for late events; updates dashboard_snapshot via psycopg2.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import psycopg2
from pyspark.sql import functions as F
from pyspark.sql import Row
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

SCHEMA = settings.postgres.metrics_schema

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()), StructField("user_id", StringType()),
    StructField("product_id", StringType()), StructField("product_name", StringType()),
    StructField("category", StringType()), StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()), StructField("total_amount", DoubleType()),
    StructField("country", StringType()), StructField("timestamp", StringType()),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType()), StructField("status", StringType()),
    StructField("amount", DoubleType()), StructField("timestamp", StringType()),
])

CLICK_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("session_id", StringType()),
    StructField("page_url", StringType()), StructField("timestamp", StringType()),
])

USER_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("timestamp", StringType()),
])


def _pg_execute(sql: str, params=None) -> None:
    conn = psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.metrics_database,
        user=settings.postgres.user,
        password=settings.postgres.password,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


def _write_jdbc(batch_df, table: str, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    (
        batch_df.write.format("jdbc")
        .option("url", settings.postgres.metrics_jdbc_url)
        .option("dbtable", f"{SCHEMA}.{table}")
        .option("user", settings.postgres.user)
        .option("password", settings.postgres.password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    logger.info("Wrote %s.%s batch=%d", SCHEMA, table, batch_id)


def _read_topic(spark, topic: str, group: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", f"ecommerce-unified-{group}")
        .option("failOnDataLoss", "false")
        .load()
    )


def _parse(kafka_df, schema):
    return (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )


def _bronze(df, name: str, ckpt: str):
    return (
        df.writeStream.format("parquet")
        .option("path", f"{settings.data_lake.bronze_path}{name}/")
        .option("checkpointLocation", f"{ckpt}/bronze_{name}")
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .trigger(processingTime=settings.spark.trigger_interval)
        .start()
    )


def main():
    logger.info(
        "Unified streaming | lake_mode=%s root=%s trigger=%s",
        settings.data_lake.mode,
        settings.data_lake.root,
        settings.spark.trigger_interval,
    )

    spark = create_spark_session("ecommerce-unified-streaming")
    spark.sparkContext.setLogLevel("WARN")
    ckpt = f"{settings.spark.checkpoint_base}/unified"
    wm = settings.spark.watermark_delay
    trigger = settings.spark.trigger_interval

    orders = _parse(_read_topic(spark, settings.kafka.orders_topic, "orders"), ORDER_SCHEMA)
    payments = _parse(_read_topic(spark, settings.kafka.payments_topic, "payments"), PAYMENT_SCHEMA)
    clicks = _parse(_read_topic(spark, settings.kafka.clicks_topic, "clicks"), CLICK_SCHEMA)
    users = _parse(_read_topic(spark, settings.kafka.users_topic, "users"), USER_SCHEMA)

    # --- Revenue + dashboard snapshot ---
    rev_agg = (
        orders.withWatermark("event_time", wm)
        .groupBy(F.window("event_time", "1 minute").alias("tw"))
        .agg(F.sum("total_amount").alias("total_revenue"), F.count("order_id").alias("order_count"))
        .select(
            F.col("tw.start").alias("window_start"), F.col("tw.end").alias("window_end"),
            "total_revenue", "order_count", F.current_timestamp().alias("updated_at"),
        )
    )

    def rev_batch(df, bid):
        _write_jdbc(df, "revenue_per_minute", bid)
        if not df.isEmpty():
            r = df.orderBy(F.desc("window_start")).first()
            _pg_execute(
                f"UPDATE {SCHEMA}.dashboard_snapshot SET total_revenue=%s, "
                f"orders_per_minute=%s, snapshot_time=NOW() WHERE id=1",
                (float(r.total_revenue), int(r.order_count)),
            )

    # --- Geographic ---
    geo_agg = (
        orders.withWatermark("event_time", wm)
        .groupBy(F.window("event_time", "1 minute").alias("tw"), F.col("country"))
        .agg(F.count("order_id").alias("order_count"), F.sum("total_amount").alias("revenue"))
        .select(
            F.col("tw.start").alias("window_start"), "country", "order_count",
            "revenue", F.current_timestamp().alias("updated_at"),
        )
    )

    def geo_batch(df, bid):
        _write_jdbc(df, "geographic_metrics", bid)
        if not df.isEmpty():
            top = df.orderBy(F.desc("revenue")).first()
            _pg_execute(
                f"UPDATE {SCHEMA}.dashboard_snapshot SET top_country=%s, snapshot_time=NOW() WHERE id=1",
                (top.country,),
            )

    # --- Top products ---
    prod_agg = (
        orders.withWatermark("event_time", wm)
        .groupBy(F.window("event_time", "1 minute").alias("tw"), F.col("product_id"))
        .agg(
            F.first("product_name").alias("product_name"),
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
        )
    )
    # Ranking per window done in foreachBatch (row_number not supported on streaming DF)
    prod_flat = prod_agg.select(
        F.col("tw.start").alias("window_start"),
        "product_id", "product_name", "units_sold", "revenue",
    )

    def prod_batch(batch_df, bid):
        if batch_df.isEmpty():
            return
        spark_sess = batch_df.sparkSession
        ranked_rows = []
        for window_row in batch_df.select("window_start").distinct().collect():
            ws = window_row.window_start
            top5 = (
                batch_df.filter(F.col("window_start") == ws)
                .orderBy(F.desc("revenue"))
                .limit(5)
                .collect()
            )
            for rank, row in enumerate(top5, 1):
                ranked_rows.append(Row(
                    window_start=ws,
                    product_id=row.product_id,
                    product_name=row.product_name,
                    units_sold=row.units_sold,
                    revenue=row.revenue,
                    rank_position=rank,
                    updated_at=None,
                ))
        if not ranked_rows:
            return
        from pyspark.sql.types import TimestampType
        out_df = spark_sess.createDataFrame(ranked_rows).withColumn(
            "updated_at", F.current_timestamp()
        )
        _write_jdbc(out_df, "top_products", bid)
        top = ranked_rows[0] if ranked_rows else None
        if top and top.product_name:
            _pg_execute(
                f"UPDATE {SCHEMA}.dashboard_snapshot SET top_product=%s, snapshot_time=NOW() WHERE id=1",
                (top.product_name,),
            )

    # --- Failed payments ---
    fail_agg = (
        payments.filter(F.col("status") == "failed")
        .withWatermark("event_time", wm)
        .groupBy(F.window("event_time", "1 minute").alias("tw"))
        .agg(F.count("payment_id").alias("failure_count"), F.sum("amount").alias("failure_amount"))
        .select(
            F.col("tw.start").alias("window_start"), F.col("tw.end").alias("window_end"),
            "failure_count", "failure_amount", F.current_timestamp().alias("updated_at"),
        )
    )

    def fail_batch(df, bid):
        _write_jdbc(df, "failed_payments", bid)
        if not df.isEmpty():
            r = df.orderBy(F.desc("window_start")).first()
            _pg_execute(
                f"UPDATE {SCHEMA}.dashboard_snapshot SET failed_payments=%s, snapshot_time=NOW() WHERE id=1",
                (int(r.failure_count),),
            )

    # --- Active users ---
    user_agg = (
        users.withWatermark("event_time", wm)
        .groupBy(F.window("event_time", "1 minute").alias("tw"))
        .agg(F.approx_count_distinct("user_id").alias("active_user_count"))
        .select(
            F.col("tw.start").alias("window_start"), F.col("tw.end").alias("window_end"),
            "active_user_count", F.current_timestamp().alias("updated_at"),
        )
    )

    def user_batch(df, bid):
        _write_jdbc(df, "active_users", bid)
        if not df.isEmpty():
            r = df.orderBy(F.desc("window_start")).first()
            _pg_execute(
                f"UPDATE {SCHEMA}.dashboard_snapshot SET active_users=%s, snapshot_time=NOW() WHERE id=1",
                (int(r.active_user_count),),
            )

    # --- Clickstream ---
    click_agg = (
        clicks.withWatermark("event_time", wm)
        .groupBy(F.window("event_time", "1 minute").alias("tw"))
        .agg(
            F.count("event_id").alias("page_views"),
            F.approx_count_distinct("session_id").alias("unique_sessions"),
            F.first("page_url").alias("top_page"),
        )
        .select(
            F.col("tw.start").alias("window_start"), F.col("tw.end").alias("window_end"),
            "page_views", "unique_sessions", "top_page",
            F.current_timestamp().alias("updated_at"),
        )
    )

    queries = [
        _bronze(orders, "orders", ckpt),
        _bronze(payments, "payments", ckpt),
        _bronze(clicks, "clicks", ckpt),
        _bronze(users, "users", ckpt),
        rev_agg.writeStream.foreachBatch(rev_batch).option("checkpointLocation", f"{ckpt}/rev")
        .outputMode("update").trigger(processingTime=trigger).start(),
        geo_agg.writeStream.foreachBatch(geo_batch).option("checkpointLocation", f"{ckpt}/geo")
        .outputMode("update").trigger(processingTime=trigger).start(),
        prod_flat.writeStream.foreachBatch(prod_batch).option("checkpointLocation", f"{ckpt}/prod")
        .outputMode("update").trigger(processingTime=trigger).start(),
        fail_agg.writeStream.foreachBatch(fail_batch).option("checkpointLocation", f"{ckpt}/fail")
        .outputMode("update").trigger(processingTime=trigger).start(),
        user_agg.writeStream.foreachBatch(user_batch).option("checkpointLocation", f"{ckpt}/users")
        .outputMode("update").trigger(processingTime=trigger).start(),
        click_agg.writeStream.foreachBatch(lambda d, i: _write_jdbc(d, "clickstream_metrics", i))
        .option("checkpointLocation", f"{ckpt}/clicks").outputMode("update")
        .trigger(processingTime=trigger).start(),
    ]

    logger.info("Started %d streaming queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
