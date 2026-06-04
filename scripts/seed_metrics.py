#!/usr/bin/env python3
"""Seed realtime metrics for dashboard demo (when Spark is still warming up)."""

import os
import random
import sys
from datetime import datetime, timedelta

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from configs.settings import settings

SCHEMA = settings.postgres.metrics_schema


def seed():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=settings.postgres.metrics_database,
        user=settings.postgres.user,
        password=settings.postgres.password,
    )
    conn.autocommit = True
    cur = conn.cursor()
    now = datetime.utcnow()

    for i in range(45):
        ws = now - timedelta(minutes=45 - i)
        we = ws + timedelta(minutes=1)
        rev = round(random.uniform(800, 8500), 2)
        orders = random.randint(8, 55)
        users = random.randint(80, 420)
        cur.execute(
            f"""INSERT INTO {SCHEMA}.revenue_per_minute
                (window_start, window_end, total_revenue, order_count)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (ws, we, rev, orders),
        )
        cur.execute(
            f"""INSERT INTO {SCHEMA}.active_users
                (window_start, window_end, active_user_count)
                VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
            (ws, we, users),
        )

    products = [
        ("SKU-001", "Wireless Headphones", 45, 3599.55),
        ("SKU-006", "Smart Watch", 22, 4399.78),
        ("SKU-009", "Mechanical Keyboard", 18, 2699.82),
    ]
    for rank, (pid, name, units, rev) in enumerate(products, 1):
        cur.execute(
            f"""INSERT INTO {SCHEMA}.top_products
                (window_start, product_id, product_name, units_sold, revenue, rank_position)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (now, pid, name, units, rev, rank),
        )

    for country, orders, rev in [("US", 120, 15000), ("UK", 85, 9500), ("CA", 60, 7200), ("DE", 45, 4800)]:
        cur.execute(
            f"""INSERT INTO {SCHEMA}.geographic_metrics
                (window_start, country, order_count, revenue)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (now, country, orders, rev),
        )

    cur.execute(
        f"""UPDATE {SCHEMA}.dashboard_snapshot SET
            total_revenue=12500, orders_per_minute=28, active_users=245,
            failed_payments=3, top_product='Wireless Headphones', top_country='US',
            snapshot_time=NOW() WHERE id=1"""
    )
    conn.close()
    print(f"Seeded {SCHEMA} metrics (45 minutes of sample windows).")


if __name__ == "__main__":
    seed()
