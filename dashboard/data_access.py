"""
Dashboard data access — reads PostgreSQL realtime schema (+ snapshot row).
"""

from datetime import datetime
from typing import Any, Dict

import pandas as pd
import psycopg2

from configs.settings import settings

SCHEMA = settings.postgres.metrics_schema


def get_connection():
    return psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.metrics_database,
        user=settings.postgres.user,
        password=settings.postgres.password,
    )


def fetch_dataframe(query: str, params=None) -> pd.DataFrame:
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception:
        return pd.DataFrame()


def get_latest_kpis() -> Dict[str, Any]:
    """Prefer denormalized snapshot (updated by Spark) for low-latency KPI cards."""
    snap = fetch_dataframe(f"SELECT * FROM {SCHEMA}.dashboard_snapshot WHERE id = 1")
    if not snap.empty:
        row = snap.iloc[0]
        return {
            "total_revenue": float(row.get("total_revenue", 0) or 0),
            "orders_per_minute": int(row.get("orders_per_minute", 0) or 0),
            "active_users": int(row.get("active_users", 0) or 0),
            "failed_payments": int(row.get("failed_payments", 0) or 0),
            "top_product": row.get("top_product") or "N/A",
            "top_country": row.get("top_country") or "N/A",
            "snapshot_time": row.get("snapshot_time") or datetime.utcnow(),
        }

    return _kpis_from_tables()


def _kpis_from_tables() -> Dict[str, Any]:
    revenue_df = fetch_dataframe(
        f"SELECT total_revenue, order_count FROM {SCHEMA}.revenue_per_minute "
        "ORDER BY window_start DESC LIMIT 1"
    )
    users_df = fetch_dataframe(
        f"SELECT active_user_count FROM {SCHEMA}.active_users "
        "ORDER BY window_start DESC LIMIT 1"
    )
    failed_df = fetch_dataframe(
        f"SELECT failure_count FROM {SCHEMA}.failed_payments "
        "ORDER BY window_start DESC LIMIT 1"
    )
    top_product_df = fetch_dataframe(
        f"SELECT product_name FROM {SCHEMA}.top_products "
        "ORDER BY window_start DESC, rank_position ASC LIMIT 1"
    )
    geo_df = fetch_dataframe(
        f"SELECT country FROM {SCHEMA}.geographic_metrics "
        "ORDER BY window_start DESC, revenue DESC LIMIT 1"
    )

    return {
        "total_revenue": float(revenue_df["total_revenue"].iloc[0]) if not revenue_df.empty else 0.0,
        "orders_per_minute": int(revenue_df["order_count"].iloc[0]) if not revenue_df.empty else 0,
        "active_users": int(users_df["active_user_count"].iloc[0]) if not users_df.empty else 0,
        "failed_payments": int(failed_df["failure_count"].iloc[0]) if not failed_df.empty else 0,
        "top_product": top_product_df["product_name"].iloc[0] if not top_product_df.empty else "N/A",
        "top_country": geo_df["country"].iloc[0] if not geo_df.empty else "N/A",
        "snapshot_time": datetime.utcnow(),
    }


def get_revenue_timeseries(minutes: int = 60) -> pd.DataFrame:
    return fetch_dataframe(
        f"""
        SELECT window_start, total_revenue, order_count
        FROM {SCHEMA}.revenue_per_minute
        WHERE window_start >= NOW() - INTERVAL '%s minutes'
        ORDER BY window_start ASC
        """,
        (minutes,),
    )


def get_active_users_timeseries(minutes: int = 60) -> pd.DataFrame:
    return fetch_dataframe(
        f"""
        SELECT window_start, active_user_count
        FROM {SCHEMA}.active_users
        WHERE window_start >= NOW() - INTERVAL '%s minutes'
        ORDER BY window_start ASC
        """,
        (minutes,),
    )


def get_top_products(minutes: int = 30) -> pd.DataFrame:
    return fetch_dataframe(
        f"""
        SELECT product_name, units_sold, revenue, rank_position
        FROM {SCHEMA}.top_products
        WHERE window_start >= NOW() - INTERVAL '%s minutes'
        ORDER BY window_start DESC, rank_position ASC
        LIMIT 10
        """,
        (minutes,),
    )


def get_geographic_metrics(minutes: int = 30) -> pd.DataFrame:
    return fetch_dataframe(
        f"""
        SELECT country, SUM(order_count) AS orders, SUM(revenue) AS revenue
        FROM {SCHEMA}.geographic_metrics
        WHERE window_start >= NOW() - INTERVAL '%s minutes'
        GROUP BY country ORDER BY revenue DESC
        """,
        (minutes,),
    )


def get_clickstream_metrics(minutes: int = 30) -> pd.DataFrame:
    return fetch_dataframe(
        f"""
        SELECT window_start, page_views, unique_sessions, top_page
        FROM {SCHEMA}.clickstream_metrics
        WHERE window_start >= NOW() - INTERVAL '%s minutes'
        ORDER BY window_start ASC
        """,
        (minutes,),
    )


def has_live_data() -> bool:
    df = fetch_dataframe(f"SELECT 1 FROM {SCHEMA}.revenue_per_minute LIMIT 1")
    return not df.empty
