# Architecture Diagrams — E-Commerce Real-Time Data Platform

Use these in README, presentations, and interviews. Three formats: Mermaid (GitHub), ASCII (terminals/docs), and README embed.

---

## 1. Mermaid Diagram (Primary)

```mermaid
flowchart TB
    subgraph INGEST["Ingestion Layer"]
        P["Python Producers<br/><i>orders · payments · clicks · users</i>"]
        PG["PostgreSQL OLTP<br/><i>customers · orders · inventory</i>"]
    end

    subgraph STREAM["Streaming Bus"]
        K["Apache Kafka<br/><i>partitioned topics · LZ4</i>"]
    end

    subgraph CDC["Change Data Capture"]
        DBZ["Debezium<br/><i>WAL / logical replication</i>"]
        KC["Kafka Connect"]
    end

    subgraph PROCESS["Stream Processing"]
        SS["Spark Structured Streaming<br/><i>watermark · 1-min windows</i>"]
    end

    subgraph LAKE["Data Lake — Medallion"]
        B["🥉 Bronze<br/>Raw Parquet"]
        S["🥈 Silver<br/>Cleaned + CDC"]
        G["🥇 Gold<br/>Business aggregates"]
    end

    subgraph SERVE["Serving Layer"]
        PGM["PostgreSQL<br/><i>realtime metrics</i>"]
        DASH["Streamlit Dashboard<br/><i>CommercePulse</i>"]
    end

    subgraph BATCH["Batch & Warehouse"]
        GLUE["AWS Glue ETL<br/><i>local: PySpark batch</i>"]
        RS["Redshift Warehouse<br/><i>local: PG warehouse schema</i>"]
        AF["Apache Airflow<br/><i>orchestration</i>"]
    end

    P -->|"JSON events"| K
    PG --> DBZ --> KC -->|"CDC topics"| K
    K --> SS
    SS --> B
    SS --> PGM
    K -.->|"optional CDC stream"| S
    B --> GLUE --> S --> G
    G --> RS
    AF --> GLUE
    AF --> SS
    PGM --> DASH
    RS -.->|"analytical SQL"| DASH

    classDef accent fill:#1a2332,stroke:#22d3ee,color:#e8edf4
    classDef storage fill:#131a24,stroke:#6366f1,color:#e8edf4
    class P,K,SS,DASH accent
    class B,S,G,PGM,RS storage
```

### Simplified Mermaid (README hero)

```mermaid
flowchart LR
    P[Python Producers] --> K[Kafka]
    PG[(Postgres OLTP)] --> D[Debezium CDC] --> K
    K --> S[Spark Streaming]
    S --> L[(Parquet Data Lake)]
    S --> M[(PostgreSQL Metrics)]
    M --> UI[Streamlit Dashboard]
    L --> E[Glue ETL] --> W[(Redshift)]
```

---

## 2. Clean Markdown / ASCII Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     E-COMMERCE REAL-TIME DATA PLATFORM                          │
└─────────────────────────────────────────────────────────────────────────────────┘

  INGESTION                    STREAMING                 PROCESSING
  ─────────                    ─────────                 ──────────

  ┌──────────────┐             ┌─────────────┐           ┌──────────────────────┐
  │   Python     │  events     │   Apache    │  consume  │  Spark Structured    │
  │  Producers   │────────────►│    Kafka    │──────────►│     Streaming        │
  │ ~15 evt/sec  │             │  5 topics   │           │  unified_streaming   │
  └──────────────┘             └──────▲──────┘           └──────────┬───────────┘
                                      │                              │
  ┌──────────────┐             ┌──────┴──────┐                       │
  │  PostgreSQL  │  WAL        │  Debezium   │                       │
  │    OLTP      │────────────►│  + Connect  │───────────────────────┘
  │ (CDC source) │             │    (CDC)    │
  └──────────────┘             └─────────────┘

                                         │
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                    ▼
             ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
             │   BRONZE    │      │  PostgreSQL │      │   SILVER    │
             │   Parquet   │      │   realtime  │      │  (CDC ref)  │
             │  data/lake  │      │   schema    │      │   Parquet   │
             └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
                    │                    │                    │
                    │             ┌──────▼──────┐             │
                    │             │ Streamlit   │             │
                    │             │ Dashboard   │             │
                    │             │ CommercePulse│            │
                    │             └─────────────┘             │
                    │                                         │
                    └──────────────┬──────────────────────────┘
                                   ▼
                            ┌─────────────┐
                            │    GOLD     │◄── Glue ETL (PySpark local)
                            │  aggregates │
                            └──────┬──────┘
                                   ▼
                            ┌─────────────┐
                            │  Redshift   │◄── Airflow orchestration
                            │  warehouse  │    (PG warehouse sim locally)
                            └─────────────┘
```

---

## 3. README-Ready Version

Copy into `README.md`:

### System Architecture

```
Producers → Kafka → Spark Structured Streaming → PostgreSQL + Parquet Lake → Dashboard
                ↑
         Postgres → Debezium CDC
```

| Stage | Technology | Output |
|-------|------------|--------|
| Ingest | Python, Kafka | 5 partitioned topics |
| Stream process | Spark Structured Streaming | 1-min windows, 10-min watermark |
| Real-time serve | PostgreSQL `realtime` | KPI tables + snapshot |
| Data lake | Parquet bronze/silver/gold | `data/lake/` |
| CDC | Debezium → Kafka | OLTP change events |
| Batch | Glue-style PySpark + Airflow | Gold tables → warehouse |
| Analytics | Redshift (sim: PostgreSQL) | Star schema |
| UI | Streamlit CommercePulse | Executive dashboard |

Full diagram: [docs/ARCHITECTURE_DIAGRAM.md](docs/ARCHITECTURE_DIAGRAM.md)

---

## Data Flow Summary

| Path | Latency | Purpose |
|------|---------|---------|
| **Hot** | ~30–60s | Operational KPIs on dashboard |
| **Warm** | Minutes | Bronze parquet for replay |
| **Cold** | Daily | Gold + warehouse for BI |

---

## Layer Reference

| Layer | Format | Written by |
|-------|--------|------------|
| Bronze | Parquet | Spark Streaming |
| Silver | Parquet | CDC / cleansing jobs |
| Gold | Parquet | Glue ETL / Spark batch |
| Warehouse | Columnar SQL | Redshift COPY (local: PG) |
