# Data Engineering Interview Guide

Deep-dive reference for the **E-Commerce Real-Time Data Platform** portfolio project. Use this to prepare system design discussions, whiteboard walks, and behavioral answers.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Walkthrough](#2-architecture-walkthrough)
3. [Kafka Design Decisions](#3-kafka-design-decisions)
4. [Spark Structured Streaming](#4-spark-structured-streaming)
5. [Watermarking & Late Data](#5-watermarking--late-data)
6. [Partitioning Strategy](#6-partitioning-strategy)
7. [CDC Concepts](#7-cdc-concepts)
8. [Why Parquet](#8-why-parquet)
9. [Bronze / Silver / Gold](#9-bronze--silver--gold)
10. [Batch vs Streaming](#10-batch-vs-streaming)
11. [Glue, Redshift & Orchestration](#11-glue-redshift--orchestration)
12. [Scaling Considerations](#12-scaling-considerations)
13. [Local vs Production](#13-local-vs-production)
14. [Sample Interview Q&A](#14-sample-interview-q-a)
15. [5-Minute Demo Script](#15-5-minute-demo-script)

---

## 1. Executive Summary

**One-liner:** Event-driven e-commerce analytics platform ingesting ~1.3M events/day through Kafka, processing with Spark Structured Streaming (watermarked windows), persisting to a Parquet medallion lake and PostgreSQL, and serving executive KPIs via a Streamlit dashboard—with CDC from PostgreSQL via Debezium and batch paths simulating Glue → Redshift.

**Business problem:** Operations and growth teams need **sub-minute visibility** into revenue, funnel health, payment failures, and geo/product performance—not just nightly reports.

**Technical outcome:** Decoupled, replayable, scalable pipeline that mirrors how mid-market companies build modern data stacks before full cloud maturity.

---

## 2. Architecture Walkthrough

### Primary data path (hot)

```
Python Producers → Kafka → Spark Structured Streaming → PostgreSQL (metrics) + Parquet (bronze)
                                                                              ↓
                                                                    Streamlit Dashboard
```

1. **Producers** emit JSON events (orders, payments, clicks, users, inventory) with business keys.
2. **Kafka** buffers and partitions streams for parallel consumption.
3. **Spark** parses JSON, applies 1-minute tumbling windows with watermark, aggregates KPIs.
4. **PostgreSQL `realtime` schema** stores windowed metrics + a single-row `dashboard_snapshot` for fast reads.
5. **Bronze Parquet** in `data/lake/bronze/` stores immutable raw history partitioned by `year/month/day`.
6. **Dashboard (CommercePulse)** polls PostgreSQL every 5 seconds.

### CDC path (warm)

```
PostgreSQL OLTP → Debezium (WAL) → Kafka Connect → Kafka CDC topics → Spark → Silver Parquet
```

Captures **insert / update / delete** on customers, products, orders, inventory without application code changes.

### Batch path (cold)

```
Bronze Parquet → Glue-style PySpark ETL → Gold Parquet → Redshift warehouse (PostgreSQL sim locally)
                     ↑
               Airflow DAGs
```

**Diagrams:** [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md) · [architecture.md](architecture.md)

---

## 3. Kafka Design Decisions

### Why Kafka (not REST polling or DB triggers)?

| Benefit | Explanation |
|---------|-------------|
| Decoupling | Producers don't know about Spark, dashboard, or warehouse |
| Buffering | Absorbs flash sales without dropping events |
| Replay | New consumer groups can reprocess history |
| Fan-out | Same stream feeds real-time, CDC, and future ML |

### Topic design

| Topic | Partitions | Key | Rationale |
|-------|------------|-----|-----------|
| `orders` | 6 | `order_id` | Order-level ordering |
| `payments` | 6 | `payment_id` | Payment lifecycle |
| `clicks` | 12 | `session_id` | Higher volume clickstream |
| `users` | 3 | `user_id` | Lower volume activity |
| `inventory_updates` | 3 | `product_id` | Stock per SKU |

### Producer settings (portfolio)

- `acks=all` — durability over speed
- LZ4 compression — bandwidth efficient
- Keyed messages — partition affinity
- Retry on `BufferError` — resilience under burst

**Interview sound bite:** *"I partition by business key so all events for one order land in one partition, preserving order for downstream stateful processing."*

---

## 4. Spark Structured Streaming

### Why Structured Streaming (not Kafka Streams alone)?

- **Unified API** with batch (same transforms for silver/gold)
- **Native Kafka connector** and Parquet sinks
- **Declarative windows** and watermarks
- **JDBC sinks** to PostgreSQL for serving layer

### What `unified_streaming.py` does

- One Spark application, **multiple streaming queries**:
  - 4 bronze Parquet sinks (orders, payments, clicks, users)
  - Revenue, geo, top products, failed payments, active users, clickstream → PostgreSQL
- **Trigger:** 30-second processing interval (configurable)
- **foreachBatch** for JDBC writes and dashboard snapshot updates

### Concepts to mention

| Concept | In this project |
|---------|-----------------|
| Source | Kafka `readStream` |
| Transform | `groupBy(window)` + aggregations |
| Sink | Parquet (bronze), JDBC (metrics) |
| Checkpoint | `/data-lake/checkpoints` — fault tolerance |
| Output mode | `update` / `append` per query |

---

## 5. Watermarking & Late Data

### Problem

Mobile clients, clock skew, or retry logic can deliver events **after** their logical window closed.

### Solution

```python
.withWatermark("event_time", "10 minutes")
```

- Engine tracks event-time progress
- Events older than watermark are **dropped from aggregations**
- Bronze layer still receives **all** events (append-only audit)

### Trade-off

| More watermark delay | Less delay |
|---------------------|------------|
| More complete aggregates | Lower latency, more dropped late events |

**Interview answer:** *"I use a 10-minute watermark for KPI windows because ops metrics can tolerate slight delay, but I keep bronze raw so we can replay or correct in batch."*

---

## 6. Partitioning Strategy

### Kafka partitions

- Parallelism = number of partitions (per topic)
- Key hash → partition → per-key ordering guarantee

### Data lake partitions

```
bronze/orders/year=2026/month=6/day=3/*.parquet
```

- **Pruning** on date for batch jobs
- Aligns with Glue/Redshift partition patterns in production

### PostgreSQL

- Time-series tables keyed by `window_start`
- Indexes on `window_start DESC` for dashboard queries

---

## 7. CDC Concepts

### What is CDC?

**Change Data Capture** propagates row-level changes from operational DBs to analytics systems.

### Why Debezium?

| Approach | Pros | Cons |
|----------|------|------|
| Timestamp polling | Simple | Misses deletes, load on DB |
| Triggers | Low latency | Invasive, hard to maintain |
| **WAL (Debezium)** | Complete, low app impact | Needs logical replication |

### This project

- PostgreSQL `wal_level=logical`
- Debezium `pgoutput` plugin
- Tables: `customers`, `products`, `orders`, `inventory_log`
- Events include `op` = c/u/d (create/update/delete)

**Interview answer:** *"CDC lets analytics stay in sync with OLTP without dual-writes in application code—critical when order status or inventory drives revenue metrics."*

---

## 8. Why Parquet

| Property | Benefit |
|----------|---------|
| Columnar | Fast aggregations on revenue, country, SKU |
| Compression | Cheaper lake storage vs raw JSON |
| Schema | Strong types for Spark/Glue |
| Ecosystem | Native in Spark, Redshift COPY, Athena |

**vs JSON in lake:** Parquet is the **analytical** format; JSON remains on the wire in Kafka.

---

## 9. Bronze / Silver / Gold

| Layer | Contents | Mutability | Consumers |
|-------|----------|------------|-----------|
| **Bronze** | Raw parsed events | Append-only | Replay, audit, data science |
| **Silver** | Cleaned, deduped, CDC-aligned | Overwrite/partition replace | Shared curated tables |
| **Gold** | Business KPIs (daily revenue, product performance) | Batch rebuild OK | BI, executives, warehouse |

**Medallion principle:** Progressive refinement increases trust and reduces duplicate cleansing logic.

**Local mapping:**

- Bronze → `data/lake/bronze/`
- Silver → `data/lake/silver/` (+ CDC)
- Gold → `data/lake/gold/` via `silver_to_gold.py`

---

## 10. Batch vs Streaming

| Dimension | Streaming | Batch |
|-----------|-----------|-------|
| Latency | 30–60 seconds | Hours / daily |
| Cost | Always-on compute | Cheaper scheduled |
| Use case | Dashboard, alerts | Finance close, cohorts |
| Correctness | Approximate windows | Full recompute |

### Lambda architecture (what this project demonstrates)

- **Speed layer:** Spark Streaming → PostgreSQL
- **Batch layer:** Spark batch / Glue → Gold → Redshift
- **Serving:** Dashboard (hot), SQL warehouse (cold)

**Interview answer:** *"Streaming answers 'what's happening now'; batch answers 'what happened officially for the quarter'—both read from the same bronze foundation."*

---

## 11. Glue, Redshift & Orchestration

### AWS Glue (reference in repo)

- **Crawlers** — schema discovery on S3
- **ETL scripts** — `glue_jobs/etl/silver_to_gold_etl.py`
- **Local sim:** `spark/batch/silver_to_gold.py`

### Amazon Redshift (reference)

- Star schema: `fact_orders`, `dim_products`, `dim_users`, `dim_date`
- **Local sim:** PostgreSQL `warehouse` schema

### Apache Airflow

- `local_batch_etl_dag` — bronze→gold + warehouse refresh
- `ecommerce_spark_streaming` — job health
- `ecommerce_glue_redshift_load` — cloud reference DAG

---

## 12. Scaling Considerations

| Bottleneck | Scale lever |
|------------|-------------|
| Ingest rate | More partitions, more producer instances |
| Stream processing | Spark executors, shuffle tuning |
| Kafka storage | Retention, tiered storage |
| Dashboard reads | Read replica, Redis cache on snapshot |
| Warehouse | Redshift dist/sort keys, Spectrum on gold |

**Order-of-magnitude for this portfolio:**

- ~15 events/sec → ~1.3M/day
- Designed to discuss **10×** with partition/executor increases without redesign

---

## 13. Local vs Production

| Production | Local portfolio |
|------------|-----------------|
| Amazon MSK | Docker Kafka |
| EMR / K8s Spark | Docker Spark |
| S3 | `data/lake/` |
| Redshift | PG `warehouse` |
| Glue | PySpark batch |
| MWAA | Docker Airflow (optional) |

**Say in interviews:** *"Architecture is production-shaped; runtime is Docker-local for cost-free demos."*

See [LOCAL_VS_CLOUD.md](LOCAL_VS_CLOUD.md).

---

## 14. Sample Interview Q&A

### System design

**Q: Design a real-time analytics system for e-commerce.**  
**A:** Producers → Kafka for buffer/decouple → stream processor for windowed aggregates → dual write to serving DB (dashboard) and data lake (history) → batch ETL to warehouse for SQL BI. Add CDC for OLTP sync. Use Airflow for orchestration.

**Q: How do you ensure data quality?**  
**A:** Schema validation in Spark, bronze immutability, dedup in silver, reconciliation jobs comparing stream totals vs batch gold, monitor failed-payment rate as a business DQ metric.

### Kafka

**Q: At-least-once vs exactly-once?**  
**A:** This project uses at-least-once from Kafka + idempotent window writes. Exactly-once would need Kafka transactions + Spark checkpoint coordination—overkill for demo KPIs.

### Spark

**Q: Why foreachBatch for PostgreSQL?**  
**A:** Fine-grained control over JDBC upserts, snapshot row updates, and ranking logic that isn't supported in pure streaming SQL (e.g. `row_number` per window).

### CDC

**Q: How do you handle DELETE events?**  
**A:** Debezium emits delete events (with tombstone or rewrite mode); silver layer applies soft-delete or Type-2 SCD in warehouse.

### Trade-offs

**Q: Why not just use Kafka + ksqlDB?**  
**A:** ksqlDB is great for simpler flows; Spark fits when you need Parquet lake, batch/stream unification, and complex multi-sink pipelines at scale.

**Q: Biggest challenge building this?**  
**A:** Coordinating multiple streaming queries with reliable checkpoints and a low-latency dashboard read path without querying bronze Parquet directly.

More Q&A: [interview_qa.md](interview_qa.md)

---

## 15. 5-Minute Demo Script

| Minute | Action |
|--------|--------|
| 0:00 | Show architecture diagram ([ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md)) |
| 0:45 | `docker compose ps` — healthy services |
| 1:15 | Open **CommercePulse** dashboard http://localhost:8501 |
| 2:00 | Spark UI http://localhost:8080 — streaming app |
| 2:45 | `ls data/lake/bronze` — Parquet bronze |
| 3:30 | Open `unified_streaming.py` — watermark + foreachBatch |
| 4:15 | Mention CDC + Glue/Redshift as reference / simulated locally |
| 4:45 | Resume bullets: 1.3M events/day, medallion lake, sub-minute KPIs |

---

## Resume Bullet Templates

- Architected event-driven pipeline processing **1.3M+ daily events** across **5 Kafka topics** with keyed partitioning and LZ4 compression  
- Implemented **Spark Structured Streaming** with **10-minute watermarks** and tumbling windows, dual-sinking to **Parquet** and **PostgreSQL**  
- Designed **medallion lakehouse** (bronze/silver/gold) and **CDC** integration (Debezium, PostgreSQL WAL)  
- Built **CommercePulse** executive dashboard (Streamlit) with real-time KPIs; orchestrated batch ETL via **Airflow** with **Glue/Redshift** reference patterns  

---

## Key Files Cheat Sheet

| File | Why open it |
|------|-------------|
| `spark/streaming/unified_streaming.py` | Core stream logic |
| `producers/kafka_producer.py` | Ingestion reliability |
| `cdc/connectors/postgres-connector.json` | CDC config |
| `configs/settings.py` | Local vs S3 modes |
| `dashboard/app.py` | Serving layer |
| `infra/postgres/init/04_warehouse.sql` | Dimensional model |

---

*Good luck in your interviews — explain the **why** behind each technology, not just the **what**.*
