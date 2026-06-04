-- =============================================================================
-- Warehouse schema (local simulation of Amazon Redshift dimensional model)
-- Loaded by batch ETL from gold parquet / realtime aggregates
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key     SERIAL PRIMARY KEY,
    product_id      VARCHAR(50) NOT NULL UNIQUE,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    unit_price      DECIMAL(12, 2)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_users (
    user_key        SERIAL PRIMARY KEY,
    user_id         VARCHAR(100) NOT NULL,
    country         VARCHAR(50),
    city            VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key       SERIAL PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    date_key        INTEGER REFERENCES warehouse.dim_date(date_key),
    user_id         VARCHAR(100),
    product_id      VARCHAR(50),
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    quantity        INTEGER NOT NULL DEFAULT 1,
    total_amount    DECIMAL(14, 2) NOT NULL,
    country         VARCHAR(50),
    order_timestamp TIMESTAMP NOT NULL,
    etl_loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.fact_payments (
    payment_key     SERIAL PRIMARY KEY,
    payment_id      VARCHAR(100) NOT NULL,
    payment_status  VARCHAR(20),
    amount          DECIMAL(14, 2) NOT NULL,
    payment_method  VARCHAR(50),
    country         VARCHAR(50),
    payment_timestamp TIMESTAMP NOT NULL,
    etl_loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.daily_revenue_summary (
    report_date     DATE PRIMARY KEY,
    total_revenue   DECIMAL(14, 2) NOT NULL,
    order_count     INTEGER NOT NULL,
    avg_order_value DECIMAL(12, 2) NOT NULL,
    unique_countries INTEGER NOT NULL DEFAULT 0,
    etl_loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed product dimension from operational catalog
INSERT INTO warehouse.dim_products (product_id, product_name, category, unit_price)
SELECT sku, name, category, price FROM ecommerce_cdc.products
ON CONFLICT (product_id) DO NOTHING;
