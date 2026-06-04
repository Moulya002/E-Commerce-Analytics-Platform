# Setup Guide

## Prerequisites

- **Docker Desktop** 4.x+ (allocate **8 GB RAM** in Settings → Resources)
- Python 3.11+ (optional — for `seed_metrics.py` only)

## Install Docker (macOS)

If you see `docker: command not found`:

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Open Docker Desktop — wait for **Running**
3. Verify: `docker --version && docker compose version`

## Core Pipeline (Recommended)

```bash
cd "/path/to/Ecommerce Project"
cp .env.example .env
./scripts/setup.sh
```

This starts:

- Zookeeper + Kafka + topic init  
- PostgreSQL (metrics + warehouse + CDC tables)  
- Spark master/worker + **auto-started unified streaming job**  
- Event producer (~15 events/sec)  
- Streamlit dashboard  

### Verify

```bash
./scripts/healthcheck.sh
```

Open http://localhost:8501 — metrics appear within **1–2 minutes**.

### Instant demo data (optional)

```bash
python3 scripts/seed_metrics.py
```

## Makefile shortcuts

```bash
make setup      # same as ./scripts/setup.sh
make health     # healthcheck
make seed       # seed PostgreSQL metrics
make logs       # tail producer + spark + dashboard
make batch      # bronze → gold ETL + warehouse
make down       # tear down
```

## Optional Profiles

### CDC (Debezium)

```bash
docker compose --profile cdc up -d
curl http://localhost:8083/connectors/ecommerce-postgres-connector/status
python3 cdc/scripts/simulate_cdc_changes.py
```

### Airflow

```bash
docker compose --profile airflow up -d
# UI: http://localhost:8088  admin / admin
# Enable DAG: local_batch_etl
```

### MinIO (S3-compatible)

```bash
# Set in .env: DATA_LAKE_MODE=s3, S3_ENDPOINT_URL=http://minio:9000
docker compose --profile s3 up -d
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `docker: command not found` | Install/start Docker Desktop |
| Dashboard empty | Wait 2 min or `python3 scripts/seed_metrics.py` |
| Spark streaming restarts | `docker logs spark-streaming` — often Kafka not ready; wait and restart: `docker compose restart spark-streaming` |
| Port in use | Stop conflicting services on 5432, 9092, 8501, 8080 |
| Reset all data | `./scripts/teardown.sh && ./scripts/setup.sh` |

## What You Do NOT Need

- AWS account or credentials (defaults)  
- Local Python venv (everything runs in containers)  
- Manual `spark-submit` (unified job auto-starts)  

## Next Steps

- Capture screenshots → `docs/screenshots/`  
- Read [PORTFOLIO.md](PORTFOLIO.md) before interviews  
- Run `make batch` to demo gold layer + warehouse
