"""
AWS Glue ETL Job: Load gold layer data into Amazon Redshift.
Uses Glue connections and COPY command via JDBC.
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "GOLD_PATH",
        "REDSHIFT_CONNECTION",
        "REDSHIFT_SCHEMA",
        "REDSHIFT_TEMP_DIR",
        "REDSHIFT_IAM_ROLE",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

gold_path = args["GOLD_PATH"]
redshift_conn = args["REDSHIFT_CONNECTION"]
schema = args["REDSHIFT_SCHEMA"]
temp_dir = args["REDSHIFT_TEMP_DIR"]
iam_role = args["REDSHIFT_IAM_ROLE"]

logger = glue_context.get_logger()

# Load gold daily revenue into Redshift fact_orders staging
daily_revenue_df = spark.read.parquet(f"{gold_path}daily_revenue/")

glue_context.write_dynamic_frame.from_options(
    frame=glue_context.create_dynamic_frame.from_catalog(
        database="temp",
        table_name="skip",
    ) if False else glue_context.create_dynamic_frame.fromDF(
        daily_revenue_df, glue_context, "daily_revenue"
    ),
    connection_type="redshift",
    connection_options={
        "dbtable": f"{schema}.stg_daily_revenue",
        "database": "ecommerce_dw",
        "redshiftTmpDir": temp_dir,
        "aws_iam_role": iam_role,
    },
)

logger.info("Redshift load completed")
job.commit()
