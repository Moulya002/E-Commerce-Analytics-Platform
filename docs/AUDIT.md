# Repository Audit (Portfolio Refactor)

## Issues Found & Fixed

| Issue | Resolution |
|-------|------------|
| LocalStack required for S3 | Replaced with `./data/lake/` local Parquet (`DATA_LAKE_MODE=local`) |
| Spark jobs not auto-started | Added `spark-streaming` service running `unified_streaming.py` |
| Separate analytics DB broke Docker init | Merged metrics into `ecommerce` DB, schemas `realtime` + `warehouse` |
| Multiple Spark scripts hard to run locally | Single `unified_streaming.py` entry point |
| Heavy Airflow/CDC in default path | Moved to Compose **profiles** (`cdc`, `airflow`, `s3`) |
| `setup.sh` required manual spark-submit | Core setup now starts streaming automatically |
| Dashboard showed demo fallback only | Added `dashboard_snapshot` updates from Spark |
| AWS credentials implied required | Marked optional; `GLUE_SIMULATED` / `REDSHIFT_SIMULATED` defaults |

## What Fully Works Locally (Core)

- ✅ Kafka + topic initialization  
- ✅ Python producers (15 evt/sec)  
- ✅ Spark Structured Streaming → PostgreSQL metrics  
- ✅ Bronze Parquet → `data/lake/bronze/`  
- ✅ Streamlit dashboard  
- ✅ Batch gold ETL (`make batch`)  
- ✅ Warehouse schema queries (`warehouse.*`)  

## What Is Simulated

- ⚡ **S3** → `data/lake/` (optional MinIO profile)  
- ⚡ **Glue** → `spark/batch/silver_to_gold.py`  
- ⚡ **Redshift** → PostgreSQL `warehouse` schema  
- ⚡ **Airflow AWS operators** → `local_batch_etl_dag.py`  

## Optional (Profiles)

- CDC: `docker compose --profile cdc up -d`  
- Airflow: `--profile airflow`  
- MinIO: `--profile s3`  

## Legacy Files (Kept for Reference)

Individual streaming jobs in `spark/streaming/{orders,payments,...}_stream.py` — superseded by `unified_streaming.py` but demonstrate per-domain separation.
