-- Real-time metrics + batch summary (same database — simpler local Docker init)
CREATE SCHEMA IF NOT EXISTS realtime;
CREATE SCHEMA IF NOT EXISTS batch;

CREATE TABLE IF NOT EXISTS realtime.revenue_per_minute (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    total_revenue   DECIMAL(14, 2) NOT NULL DEFAULT 0,
    order_count     INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS realtime.active_users (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    active_user_count INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS realtime.top_products (
    window_start    TIMESTAMP NOT NULL,
    product_id      VARCHAR(50) NOT NULL,
    product_name    VARCHAR(255),
    units_sold      INTEGER NOT NULL DEFAULT 0,
    revenue         DECIMAL(14, 2) NOT NULL DEFAULT 0,
    rank_position   INTEGER NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, product_id)
);

CREATE TABLE IF NOT EXISTS realtime.failed_payments (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    failure_count   INTEGER NOT NULL DEFAULT 0,
    failure_amount  DECIMAL(14, 2) NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS realtime.clickstream_metrics (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    page_views      INTEGER NOT NULL DEFAULT 0,
    unique_sessions INTEGER NOT NULL DEFAULT 0,
    top_page        VARCHAR(255),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS realtime.geographic_metrics (
    window_start    TIMESTAMP NOT NULL,
    country         VARCHAR(50) NOT NULL,
    order_count     INTEGER NOT NULL DEFAULT 0,
    revenue         DECIMAL(14, 2) NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, country)
);

CREATE TABLE IF NOT EXISTS realtime.dashboard_snapshot (
    id              INTEGER PRIMARY KEY DEFAULT 1,
    total_revenue   DECIMAL(14, 2) NOT NULL DEFAULT 0,
    orders_per_minute INTEGER NOT NULL DEFAULT 0,
    active_users    INTEGER NOT NULL DEFAULT 0,
    failed_payments INTEGER NOT NULL DEFAULT 0,
    top_product     VARCHAR(255),
    top_country     VARCHAR(50),
    snapshot_time   TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);

INSERT INTO realtime.dashboard_snapshot (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS batch.daily_revenue_summary (
    report_date     DATE NOT NULL PRIMARY KEY,
    total_revenue   DECIMAL(14, 2) NOT NULL,
    order_count     INTEGER NOT NULL,
    avg_order_value DECIMAL(12, 2) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_revenue_window ON realtime.revenue_per_minute (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_active_users_window ON realtime.active_users (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_geo_window ON realtime.geographic_metrics (window_start DESC);
