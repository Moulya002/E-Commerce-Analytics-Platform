# Portfolio & Demo Playbook

## Elevator Pitch (30 seconds)

> I built a real-time e-commerce analytics platform: Python producers stream ~1.3 million events per day into Kafka, Spark Structured Streaming computes windowed KPIs with watermarking, results land in a partitioned Parquet data lake and PostgreSQL, and a Streamlit dashboard shows live revenue and funnel metrics. CDC from PostgreSQL via Debezium, batch ETL simulates Glue, and a warehouse schema simulates Redshift — all runnable on Docker for interviews.

## What to Show (in order)

| Step | What | Why it impresses |
|------|------|------------------|
| 1 | `docker compose ps` | Full stack running locally |
| 2 | Dashboard http://localhost:8501 | Business-facing output |
| 3 | Spark UI http://localhost:8080 | Real Structured Streaming job |
| 4 | `ls -R data/lake/bronze` | Medallion bronze Parquet |
| 5 | `docker logs event-producer --tail 5` | Live Kafka ingestion |
| 6 | `docs/architecture.md` diagram | System design fluency |
| 7 | `spark/streaming/unified_streaming.py` | Code depth (watermark, foreachBatch) |
| 8 | Optional: `make cdc` + connector status | CDC understanding |

## Screenshots / Video to Capture

1. **Dashboard** — KPI cards + revenue chart (wide screenshot)  
2. **Spark UI** — one active `ecommerce-unified-streaming` application  
3. **Terminal** — `healthcheck.sh` all green  
4. **VS Code** — project tree + unified_streaming.py visible  
5. **Optional 60s screen recording** — refresh dashboard, show numbers changing  

Save under `docs/screenshots/` and embed in GitHub README.

## Strongest Resume Bullets

Use these (adjust numbers if you change `PRODUCER_EVENTS_PER_SECOND`):

- Architected and deployed an event-driven analytics platform ingesting **1.3M+ daily events** across **5 Kafka topics** with keyed partitioning and LZ4 compression  
- Built **Spark Structured Streaming** pipelines with **watermarking** and **30-second tumbling windows** delivering sub-minute KPIs to PostgreSQL and Parquet data lake  
- Implemented **medallion lakehouse** (bronze/silver/gold) with **date-partitioned Parquet** for scalable batch and streaming workloads  
- Developed **CDC integration** (Debezium, PostgreSQL WAL) propagating OLTP changes to Kafka and silver lake layers  
- Created **real-time Streamlit dashboard** (revenue, geo, product rankings, payment failures) backed by streaming aggregations  
- Authored **Airflow orchestration** and **AWS Glue/Redshift reference ETL** with local PySpark/PostgreSQL simulation for cost-free demos  

## GitHub Polish Checklist

- [ ] Add 2–4 screenshots to README  
- [ ] Pin repository on GitHub profile  
- [ ] Add topics: `kafka`, `spark`, `data-engineering`, `streamlit`, `debezium`, `data-lake`  
- [ ] One-line repo description: *Real-time e-commerce data platform: Kafka, Spark, CDC, lakehouse, Streamlit*  
- [ ] Link to LinkedIn / resume PDF  

## Before an Interview

```bash
./scripts/setup.sh
sleep 120
./scripts/healthcheck.sh
python3 scripts/seed_metrics.py   # backup if Spark slow
```

Test dashboard in browser. Skim `docs/interview_qa.md`.
