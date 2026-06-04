# Amazon Redshift (Reference / Simulated Locally)

The dimensional model in `schema/01_create_schema.sql` mirrors a production Redshift warehouse.

**Local simulation:** PostgreSQL schema `warehouse` (see `infra/postgres/init/04_warehouse.sql`)

| Redshift | Local |
|----------|-------|
| `COPY` from S3 | Batch ETL + SQL insert |
| `DISTKEY` / `SORTKEY` | Standard PostgreSQL indexes |
| Spectrum / external tables | Parquet in `./data/lake/gold/` |

Sample analytics SQL: `queries/analytics_queries.sql` — adapt schema prefix to `warehouse.` locally.
