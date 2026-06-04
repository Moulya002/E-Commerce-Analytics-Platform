"""
SparkSession factory — supports local parquet data lake and optional S3/MinIO.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession

from configs.settings import settings


def create_spark_session(
    app_name: Optional[str] = None,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    name = app_name or settings.spark.app_name
    lake = settings.data_lake

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.1",
    ]
    if lake.mode == "s3":
        packages.extend([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ])

    builder = (
        SparkSession.builder
        .appName(name)
        .master(settings.spark.master)
        .config("spark.sql.streaming.checkpointLocation", settings.spark.checkpoint_base)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars.packages", ",".join(packages))
    )

    if lake.mode == "s3" and lake.endpoint_url:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", lake.endpoint_url)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
