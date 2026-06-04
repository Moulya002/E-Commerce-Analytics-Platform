# Data Lake Layout (Local Portfolio Mode)

## Default: Local Parquet (`DATA_LAKE_MODE=local`)

```
data/lake/
├── bronze/          # Spark streaming append (partitioned)
│   ├── orders/year=2026/month=6/day=3/*.parquet
│   ├── payments/
│   ├── clicks/
│   └── users/
├── silver/          # CDC events (optional profile)
└── gold/            # Batch ETL output
    └── daily_revenue/order_date=2026-06-03/*.parquet
```

**Production equivalent:** `s3://ecommerce-data-lake/bronze/...`

## Partitioning

| Layer | Keys |
|-------|------|
| Bronze | `year`, `month`, `day` |
| Gold | `order_date` |

## Inspect locally

```bash
find data/lake/bronze -name "*.parquet" | head
docker exec spark-streaming ls -la /data-lake/bronze/orders/
```

## Optional S3 mode

Set `DATA_LAKE_MODE=s3` and start MinIO: `docker compose --profile s3 up -d`
