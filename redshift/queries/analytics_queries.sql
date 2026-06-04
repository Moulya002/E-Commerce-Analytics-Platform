-- =============================================================================
-- Sample Analytical SQL Queries for Redshift Data Warehouse
-- =============================================================================

-- 1. Daily revenue trend (last 30 days)
SELECT
    d.full_date,
    d.day_name,
    SUM(f.total_amount) AS daily_revenue,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(DISTINCT f.user_key) AS unique_customers
FROM ecommerce_dw.fact_orders f
JOIN ecommerce_dw.dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC;


-- 2. Top 10 products by revenue (current month)
SELECT
    p.product_name,
    p.category,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.order_id) AS order_count
FROM ecommerce_dw.fact_orders f
JOIN ecommerce_dw.dim_products p ON f.product_key = p.product_key
JOIN ecommerce_dw.dim_date d ON f.date_key = d.date_key
WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND d.month = EXTRACT(MONTH FROM CURRENT_DATE)
  AND p.is_current = TRUE
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- 3. Revenue by country (geographic analysis)
SELECT
    COALESCE(f.shipping_country, u.country) AS country,
    SUM(f.total_amount) AS total_revenue,
    COUNT(f.order_key) AS order_count,
    AVG(f.total_amount) AS avg_order_value
FROM ecommerce_dw.fact_orders f
LEFT JOIN ecommerce_dw.dim_users u ON f.user_key = u.user_key
GROUP BY COALESCE(f.shipping_country, u.country)
ORDER BY total_revenue DESC;


-- 4. Payment failure rate by method
SELECT
    payment_method,
    COUNT(*) AS total_payments,
    SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) AS failed_payments,
    ROUND(
        100.0 * SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS failure_rate_pct,
    SUM(CASE WHEN payment_status = 'failed' THEN amount ELSE 0 END) AS failed_amount
FROM ecommerce_dw.fact_payments
WHERE payment_timestamp >= DATEADD(day, -7, CURRENT_DATE)
GROUP BY payment_method
ORDER BY failure_rate_pct DESC;


-- 5. Cohort analysis: monthly active buyers
SELECT
    DATE_TRUNC('month', d.full_date) AS order_month,
    COUNT(DISTINCT f.user_key) AS active_buyers,
    SUM(f.total_amount) AS monthly_revenue,
    AVG(f.total_amount) AS avg_order_value
FROM ecommerce_dw.fact_orders f
JOIN ecommerce_dw.dim_date d ON f.date_key = d.date_key
GROUP BY DATE_TRUNC('month', d.full_date)
ORDER BY order_month DESC;


-- 6. Year-over-year revenue comparison
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month,
        SUM(f.total_amount) AS revenue
    FROM ecommerce_dw.fact_orders f
    JOIN ecommerce_dw.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    curr.year,
    curr.month,
    curr.revenue AS current_revenue,
    prev.revenue AS previous_year_revenue,
    ROUND(
        100.0 * (curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0),
        2
    ) AS yoy_growth_pct
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev
    ON curr.month = prev.month
    AND curr.year = prev.year + 1
ORDER BY curr.year DESC, curr.month DESC;
