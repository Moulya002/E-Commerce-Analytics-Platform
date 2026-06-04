-- =============================================================================
-- Amazon Redshift Dimensional Data Warehouse Schema
-- E-Commerce Analytics Platform
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ecommerce_dw;

-- Date dimension
CREATE TABLE ecommerce_dw.dim_date (
    date_key        INTEGER NOT NULL PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    day_of_month    SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE
)
DISTSTYLE ALL
SORTKEY (full_date);

-- User dimension
CREATE TABLE ecommerce_dw.dim_users (
    user_key        BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id         VARCHAR(100) NOT NULL,
    country         VARCHAR(50),
    city            VARCHAR(100),
    first_seen_date DATE,
    is_active       BOOLEAN DEFAULT TRUE,
    effective_from  TIMESTAMP NOT NULL DEFAULT GETDATE(),
    effective_to    TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE
)
DISTSTYLE ALL
SORTKEY (user_id);

-- Product dimension
CREATE TABLE ecommerce_dw.dim_products (
    product_key     BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_id      VARCHAR(50) NOT NULL,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    sku             VARCHAR(50),
    unit_price      DECIMAL(12,2),
    is_active       BOOLEAN DEFAULT TRUE,
    effective_from  TIMESTAMP NOT NULL DEFAULT GETDATE(),
    effective_to    TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE
)
DISTSTYLE ALL
SORTKEY (product_id);

-- Orders fact table
CREATE TABLE ecommerce_dw.fact_orders (
    order_key       BIGINT IDENTITY(1,1),
    order_id        VARCHAR(100) NOT NULL,
    date_key        INTEGER NOT NULL REFERENCES ecommerce_dw.dim_date(date_key),
    user_key        BIGINT REFERENCES ecommerce_dw.dim_users(user_key),
    product_key     BIGINT REFERENCES ecommerce_dw.dim_products(product_key),
    order_status    VARCHAR(50),
    quantity        INTEGER NOT NULL DEFAULT 1,
    unit_price      DECIMAL(12,2) NOT NULL,
    total_amount    DECIMAL(14,2) NOT NULL,
    discount_amount DECIMAL(12,2) DEFAULT 0,
    tax_amount      DECIMAL(12,2) DEFAULT 0,
    shipping_country VARCHAR(50),
    order_timestamp TIMESTAMP NOT NULL,
    etl_loaded_at   TIMESTAMP NOT NULL DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (date_key)
SORTKEY (order_timestamp, date_key);

-- Staging tables for ETL loads
CREATE TABLE ecommerce_dw.stg_daily_revenue (
    order_date          DATE,
    country             VARCHAR(50),
    category            VARCHAR(100),
    total_revenue       DECIMAL(14,2),
    order_count         INTEGER,
    avg_order_value     DECIMAL(12,2),
    unique_customers    INTEGER,
    etl_processed_at    TIMESTAMP
);

-- Payment fact table
CREATE TABLE ecommerce_dw.fact_payments (
    payment_key     BIGINT IDENTITY(1,1),
    payment_id      VARCHAR(100) NOT NULL,
    order_id        VARCHAR(100),
    date_key        INTEGER REFERENCES ecommerce_dw.dim_date(date_key),
    user_key        BIGINT REFERENCES ecommerce_dw.dim_users(user_key),
    amount          DECIMAL(14,2) NOT NULL,
    payment_method  VARCHAR(50),
    payment_status  VARCHAR(20),
    failure_reason  VARCHAR(100),
    payment_timestamp TIMESTAMP NOT NULL,
    etl_loaded_at   TIMESTAMP NOT NULL DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (date_key)
SORTKEY (payment_timestamp);
