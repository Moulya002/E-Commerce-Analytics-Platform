# AWS Glue (Reference / Simulated Locally)

In **production**, these scripts run on AWS Glue with crawlers targeting S3.

In **local portfolio mode**, the same patterns are implemented by:

| AWS Glue Component | Local Equivalent |
|--------------------|------------------|
| Glue Crawler | Manual schema / `spark.read.parquet` |
| Glue ETL Job | `spark/batch/silver_to_gold.py` |
| Glue Data Catalog | Documented in `infra/data_lake/README.md` |
| Redshift load | `scripts/run_batch_etl.sh` → PostgreSQL `warehouse` schema |

## Files

- `crawlers/` — JSON definitions for AWS deployment
- `etl/silver_to_gold_etl.py` — PySpark script (deploy to Glue)
- `etl/redshift_load_etl.py` — Redshift COPY pattern (reference)

## Interview Talking Point

> "Glue jobs are included as production-ready reference code; locally I run equivalent PySpark batch jobs and load a PostgreSQL warehouse schema that mirrors Redshift."
