-- =============================================================================
-- Redshift COPY commands to load gold layer data from S3
-- Run after Glue ETL produces gold parquet files
-- =============================================================================

-- Load staging daily revenue from S3
COPY ecommerce_dw.stg_daily_revenue
FROM 's3://ecommerce-data-lake/gold/daily_revenue/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftCopyRole'
FORMAT AS PARQUET
COMPUPDATE ON
STATUPDATE ON;

-- Merge staging into fact_orders (upsert pattern)
BEGIN TRANSACTION;

DELETE FROM ecommerce_dw.fact_orders
WHERE date_key IN (
    SELECT DISTINCT
        CAST(TO_CHAR(order_date, 'YYYYMMDD') AS INTEGER)
    FROM ecommerce_dw.stg_daily_revenue
);

-- Insert transformed records (simplified - production would use MERGE)
INSERT INTO ecommerce_dw.fact_orders (
    order_id, date_key, total_amount, quantity, unit_price,
    order_status, shipping_country, order_timestamp
)
SELECT
    'AGG-' || order_date || '-' || country AS order_id,
    CAST(TO_CHAR(order_date, 'YYYYMMDD') AS INTEGER),
    total_revenue,
    order_count,
    avg_order_value,
    'aggregated',
    country,
    order_date::TIMESTAMP
FROM ecommerce_dw.stg_daily_revenue;

TRUNCATE TABLE ecommerce_dw.stg_daily_revenue;

END TRANSACTION;

ANALYZE ecommerce_dw.fact_orders;
