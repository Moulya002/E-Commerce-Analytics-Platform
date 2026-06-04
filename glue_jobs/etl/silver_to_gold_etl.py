"""
AWS Glue ETL Job: Transform silver layer parquet to gold analytics tables.
Deploy to AWS Glue and run via Airflow or Glue triggers.

Job parameters (passed at runtime):
  --JOB_NAME
  --SOURCE_PATH   (s3://ecommerce-data-lake/silver/)
  --TARGET_PATH   (s3://ecommerce-data-lake/gold/)
  --DATABASE      (ecommerce_analytics)
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "DATABASE"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source_path = args["SOURCE_PATH"]
target_path = args["TARGET_PATH"]
database = args["DATABASE"]

logger = glue_context.get_logger()
logger.info(f"Starting Glue ETL | source={source_path} target={target_path}")


def clean_orders(df):
    """Silver transformation: deduplicate and standardize order events."""
    return (
        df
        .dropDuplicates(["order_id"])
        .filter(F.col("total_amount").isNotNull() & (F.col("total_amount") > 0))
        .withColumn("order_date", F.to_date(F.col("timestamp")))
        .withColumn(
            "order_status_normalized",
            F.lower(F.trim(F.col("order_status"))),
        )
    )


def build_daily_revenue(orders_df):
    """Gold table: daily revenue aggregates."""
    return (
        orders_df
        .groupBy("order_date", "country", "category")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("etl_processed_at", F.current_timestamp())
    )


def build_product_performance(orders_df):
    """Gold table: product-level performance metrics."""
    return (
        orders_df
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("unique_buyers"),
        )
        .withColumn("etl_processed_at", F.current_timestamp())
        .orderBy(F.desc("total_revenue"))
    )


# Read silver orders (from Spark streaming silver output or bronze promotion)
try:
    orders_dynamic = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name="bronze_orders",
    )
    orders_df = orders_dynamic.toDF()
except Exception:
    logger.info("Catalog table not found, reading from S3 directly")
    orders_df = spark.read.parquet(f"{source_path}orders/")

cleaned_orders = clean_orders(orders_df)

daily_revenue = build_daily_revenue(cleaned_orders)
product_performance = build_product_performance(cleaned_orders)

daily_revenue.write.mode("overwrite").partitionBy("order_date").parquet(
    f"{target_path}daily_revenue/"
)
product_performance.write.mode("overwrite").parquet(
    f"{target_path}product_performance/"
)

logger.info("Glue ETL completed successfully")
job.commit()
