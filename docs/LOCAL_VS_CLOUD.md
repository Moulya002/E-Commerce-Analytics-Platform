# Local vs Cloud: What Is Real vs Simulated

This project is designed as a **portfolio platform** — concepts match production AWS architectures, but everything runs on Docker without cloud spend.

## Fully Functional Locally (Core Demo)

| Component | Implementation |
|-----------|----------------|
| Event ingestion | `producers/` → Kafka |
| Stream processing | `spark/streaming/unified_streaming.py` |
| Metrics sink | PostgreSQL `realtime` schema |
| Data lake bronze | `./data/lake/bronze/**/*.parquet` |
| Dashboard | Streamlit on port 8501 |
| Batch gold ETL | `spark/batch/silver_to_gold.py` |
| Warehouse | PostgreSQL `warehouse` schema |

## Simulated / Reference Only

| AWS Service | Local substitute | Reference code |
|-------------|------------------|----------------|
| **S3** | `data/lake/` directory (or optional MinIO `--profile s3`) | `infra/data_lake/` |
| **Glue Crawler** | `spark.read.parquet` infers schema | `glue_jobs/crawlers/` |
| **Glue ETL** | `spark/batch/silver_to_gold.py` | `glue_jobs/etl/` |
| **Redshift** | PostgreSQL `warehouse` tables | `redshift/schema/` |
| **MSK** | Docker Kafka | `docker-compose.yml` |
| **EMR** | Docker Spark cluster | `spark-master` / `spark-worker` |
| **MWAA** | Docker Airflow (`--profile airflow`) | `airflow/dags/` |

## Environment Variables

```bash
DATA_LAKE_MODE=local      # default — no AWS credentials needed
GLUE_SIMULATED=true
REDSHIFT_SIMULATED=true
```

To experiment with S3-compatible storage:

```bash
DATA_LAKE_MODE=s3
S3_ENDPOINT_URL=http://minio:9000
docker compose --profile s3 up -d
```

## Production Migration Path

1. Replace `DATA_LAKE_MODE=local` with real S3 bucket  
2. Deploy Spark on EMR or Kubernetes  
3. Use Amazon MSK for Kafka  
4. Run `glue_jobs/etl/*.py` on AWS Glue  
5. Load `redshift/schema/` into Redshift cluster  
6. Deploy Airflow on MWAA  

The **architecture diagram stays the same** — only the runtime changes.
