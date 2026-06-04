-- =============================================================================
-- Operational PostgreSQL Schema (CDC Source + Reference Data)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ecommerce_cdc;

-- Customers
CREATE TABLE ecommerce_cdc.customers (
    customer_id     SERIAL PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    country         VARCHAR(50) NOT NULL DEFAULT 'US',
    city            VARCHAR(100),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Products / Inventory
CREATE TABLE ecommerce_cdc.products (
    product_id      SERIAL PRIMARY KEY,
    sku             VARCHAR(50) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    price           DECIMAL(12, 2) NOT NULL,
    stock_quantity  INTEGER NOT NULL DEFAULT 0,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Orders (operational - CDC captures status changes)
CREATE TABLE ecommerce_cdc.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES ecommerce_cdc.customers(customer_id),
    order_status    VARCHAR(50) NOT NULL DEFAULT 'pending',
    total_amount    DECIMAL(12, 2) NOT NULL,
    shipping_country VARCHAR(50),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE ecommerce_cdc.order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES ecommerce_cdc.orders(order_id),
    product_id      INTEGER NOT NULL REFERENCES ecommerce_cdc.products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      DECIMAL(12, 2) NOT NULL
);

-- Inventory audit log (CDC target)
CREATE TABLE ecommerce_cdc.inventory_log (
    log_id          SERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL REFERENCES ecommerce_cdc.products(product_id),
    change_type     VARCHAR(20) NOT NULL,
    quantity_delta  INTEGER NOT NULL,
    previous_stock  INTEGER NOT NULL,
    new_stock       INTEGER NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed reference data
INSERT INTO ecommerce_cdc.customers (email, first_name, last_name, country, city) VALUES
    ('alice@example.com', 'Alice', 'Johnson', 'US', 'New York'),
    ('bob@example.com', 'Bob', 'Smith', 'UK', 'London'),
    ('carol@example.com', 'Carol', 'Lee', 'CA', 'Toronto'),
    ('david@example.com', 'David', 'Kim', 'US', 'San Francisco'),
    ('emma@example.com', 'Emma', 'Brown', 'DE', 'Berlin');

INSERT INTO ecommerce_cdc.products (sku, name, category, price, stock_quantity) VALUES
    ('SKU-001', 'Wireless Headphones', 'Electronics', 79.99, 500),
    ('SKU-002', 'Running Shoes', 'Footwear', 129.99, 300),
    ('SKU-003', 'Coffee Maker', 'Home', 49.99, 200),
    ('SKU-004', 'Laptop Stand', 'Electronics', 34.99, 450),
    ('SKU-005', 'Yoga Mat', 'Sports', 29.99, 600),
    ('SKU-006', 'Smart Watch', 'Electronics', 199.99, 150),
    ('SKU-007', 'Backpack', 'Accessories', 59.99, 400),
    ('SKU-008', 'Desk Lamp', 'Home', 39.99, 350);

INSERT INTO ecommerce_cdc.orders (customer_id, order_status, total_amount, shipping_country) VALUES
    (1, 'delivered', 159.98, 'US'),
    (2, 'shipped', 129.99, 'UK'),
    (3, 'pending', 79.99, 'CA');

INSERT INTO ecommerce_cdc.order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 2, 79.99),
    (2, 2, 1, 129.99),
    (3, 1, 1, 79.99);

-- Publication for logical replication (Debezium)
CREATE PUBLICATION dbz_publication FOR TABLE
    ecommerce_cdc.customers,
    ecommerce_cdc.products,
    ecommerce_cdc.orders,
    ecommerce_cdc.inventory_log;
