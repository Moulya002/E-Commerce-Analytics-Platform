# Interview Q&A — Data Engineering Concepts

## Kafka

**Q: Why Kafka instead of polling a database?**  
**A:** Kafka decouples producers from consumers, buffers traffic spikes, and allows multiple independent consumer groups (Spark, CDC replay, future ML features) to read the same stream at their own pace.

**Q: How did you partition topics?**  
**A:** Business keys — `order_id`, `session_id`, `product_id` — so related events stay ordered per partition. Higher-volume `clicks` uses 12 partitions vs 3 for `users`.

**Q: What guarantees do you rely on?**  
**A:** At-least-once from Kafka + Spark checkpointing. Dashboard aggregations are idempotent per time window (append to windowed tables).

---

## Spark Structured Streaming

**Q: Why Spark Structured Streaming?**  
**A:** Single API for batch and stream, native Kafka connector, built-in window/watermark support, and JDBC sinks for serving layers.

**Q: Explain watermarking in your project.**  
**A:** We use a **10-minute watermark** on `event_time`. Events older than 10 minutes are dropped from aggregations but still land in bronze Parquet for audit/replay.

**Q: Batch vs streaming tradeoff?**  
**A:** Streaming gives **~30–60s KPI latency** for operations. Batch (nightly) builds gold aggregates and warehouse tables — cheaper for heavy joins and historical reporting.

---

## CDC (Debezium)

**Q: What is CDC and why Debezium?**  
**A:** Change Data Capture streams **insert/update/delete** from PostgreSQL WAL without application changes. Debezium is log-based — lower load than query-based polling and captures deletes.

**Q: What tables do you capture?**  
**A:** `customers`, `products`, `orders`, `inventory_log` in schema `ecommerce_cdc`.

---

## Data Lake (Bronze / Silver / Gold)

**Q: Why medallion architecture?**  
**A:**  
- **Bronze:** immutable raw events (replay, compliance)  
- **Silver:** cleaned, deduplicated, CDC-aligned  
- **Gold:** business aggregates for BI  

**Q: Why Parquet?**  
**A:** Columnar compression, predicate pushdown on `year/month/day` partitions, and native Spark support.

---

## Local vs AWS

**Q: Did you deploy to AWS?**  
**A:** The repo runs fully local for cost and demo reliability. Glue and Redshift are **reference implementations**; locally PySpark batch and PostgreSQL `warehouse` schema demonstrate the same patterns.

---

## System Design

**Q: How do you handle failed payments?**  
**A:** Dedicated Spark branch filters `status=failed`, aggregates per minute, writes to `realtime.failed_payments`, updates dashboard snapshot.

**Q: How would you scale to 10x traffic?**  
**A:** Increase Kafka partitions, add Spark executors, partition data lake by date, move metrics hot path to Redis if needed, keep warehouse on columnar store.

---

## Behavioral

**Q: What was the hardest part?**  
**A:** Coordinating multiple streaming queries with consistent checkpointing and ensuring the dashboard reads low-latency snapshot rows without polling bronze Parquet.

**Q: What would you improve next?**  
**A:** Schema registry (Avro), exactly-once Kafka transactions, dbt on gold layer, and Grafana for ops metrics.
