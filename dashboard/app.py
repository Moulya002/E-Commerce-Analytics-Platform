"""
CommercePulse — Executive real-time analytics dashboard.
Streamlit + Plotly | Production portfolio UI
"""

import time
from datetime import datetime, timezone

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from configs.settings import settings
from dashboard.data_access import (
    get_active_users_timeseries,
    get_clickstream_metrics,
    get_geographic_metrics,
    get_latest_kpis,
    get_revenue_timeseries,
    get_top_products,
    has_live_data,
)
from dashboard.ui_theme import (
    BRAND_NAME,
    BRAND_TAGLINE,
    COLORS,
    apply_plotly_theme,
    inject_global_css,
    kpi_card_html,
    section_divider,
    status_pill,
)

st.set_page_config(
    page_title=f"{BRAND_NAME} | Real-Time Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

REFRESH_SECONDS = settings.dashboard.refresh_seconds


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def load_metrics(time_window: int):
    return {
        "kpis": get_latest_kpis(),
        "revenue_ts": get_revenue_timeseries(time_window),
        "users_ts": get_active_users_timeseries(time_window),
        "top_products": get_top_products(min(time_window, 60)),
        "geo": get_geographic_metrics(min(time_window, 60)),
        "clicks": get_clickstream_metrics(min(time_window, 60)),
        "live": has_live_data(),
    }


def render_brand_header() -> None:
    st.markdown(
        f"""
        <div class="brand-header">
            <div class="brand-logo">CP</div>
            <div>
                <p class="brand-title">{BRAND_NAME}</p>
                <p class="brand-subtitle">{BRAND_TAGLINE}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_stream_banner(live: bool, snapshot_time, auto_refresh: bool) -> None:
    ts = snapshot_time
    if hasattr(ts, "strftime"):
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    live_pill = status_pill("LIVE STREAM" if live else "WARMING UP", "live" if live else "warn", pulse=live and auto_refresh)
    refresh_pill = status_pill(f"Auto-refresh {REFRESH_SECONDS}s", "live" if auto_refresh else "warn", pulse=auto_refresh)

    st.markdown(
        f"""
        <div class="stream-banner">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
                {live_pill}
                {refresh_pill}
                <span style="color:{COLORS['text_muted']};font-size:0.8rem;">
                    Source: PostgreSQL · Spark Structured Streaming
                </span>
            </div>
            <span style="color:{COLORS['text_muted']};font-size:0.78rem;">
                Last snapshot: {ts_str}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_row(kpis: dict) -> None:
    cards = [
        ("Revenue / Min", f"${kpis['total_revenue']:,.2f}", "delay-1", "accent",
         "Rolling 1-minute revenue from order events"),
        ("Orders / Min", f"{kpis['orders_per_minute']:,}", "delay-2", "",
         "Order count in the latest processing window"),
        ("Active Users", f"{kpis['active_users']:,}", "delay-3", "success",
         "Approximate distinct users from activity stream"),
        ("Failed Payments", f"{kpis['failed_payments']:,}", "delay-4",
         "danger" if kpis["failed_payments"] > 0 else "",
         "Failed payment events in the latest window"),
        ("Top Product", (kpis["top_product"] or "N/A")[:28], "delay-5", "",
         "Highest revenue product in recent window"),
    ]
    cols = st.columns(5, gap="medium")
    for col, (label, value, delay, vclass, tip) in zip(cols, cards):
        with col:
            st.markdown(kpi_card_html(label, value, delay, vclass, tip), unsafe_allow_html=True)


def render_revenue_chart(df) -> None:
    st.markdown('<div class="chart-panel"><p class="chart-panel-title">Revenue & Order Velocity</p>', unsafe_allow_html=True)
    if df.empty:
        st.markdown('<div class="skeleton"></div>', unsafe_allow_html=True)
        st.caption("Awaiting Spark streaming aggregates…")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["window_start"], y=df["total_revenue"], name="Revenue ($)",
        line=dict(color=COLORS["accent"], width=2.5),
        fill="tozeroy", fillcolor="rgba(34, 211, 238, 0.08)",
        hovertemplate="%{x}<br>Revenue: $%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=df["window_start"], y=df["order_count"], name="Orders",
        yaxis="y2", opacity=0.55, marker_color=COLORS["accent_secondary"],
        hovertemplate="%{x}<br>Orders: %{y}<extra></extra>",
    ))
    fig.update_layout(
        yaxis=dict(title="Revenue ($)", side="left", tickformat="$,.0f"),
        yaxis2=dict(title="Orders", overlaying="y", side="right", showgrid=False),
        hovermode="x unified",
    )
    apply_plotly_theme(fig, height=400)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)


def render_users_chart(df) -> None:
    st.markdown('<div class="chart-panel"><p class="chart-panel-title">Active Users</p>', unsafe_allow_html=True)
    if df.empty:
        st.markdown('<div class="skeleton" style="height:220px"></div>', unsafe_allow_html=True)
        return
    fig = px.area(df, x="window_start", y="active_user_count")
    fig.update_traces(line_color=COLORS["success"], fillcolor="rgba(52, 211, 153, 0.12)")
    fig.update_layout(yaxis_title="Users", xaxis_title="")
    apply_plotly_theme(fig, height=280)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)


def render_top_products(df) -> None:
    st.markdown('<div class="chart-panel"><p class="chart-panel-title">Top Products by Revenue</p>', unsafe_allow_html=True)
    if df.empty:
        st.info("No product rankings yet.", icon="📦")
        return
    agg = df.groupby("product_name", as_index=False).agg({"revenue": "sum", "units_sold": "sum"})
    agg = agg.sort_values("revenue", ascending=True).tail(8)
    fig = px.bar(agg, x="revenue", y="product_name", orientation="h", color="units_sold",
                 color_continuous_scale=["#1e3a5f", COLORS["accent"]])
    fig.update_layout(xaxis_title="Revenue ($)", yaxis_title="", coloraxis_showscale=False)
    apply_plotly_theme(fig, height=360)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)


def render_geo_charts(df) -> None:
    st.markdown('<div class="chart-panel"><p class="chart-panel-title">Geographic Performance</p>', unsafe_allow_html=True)
    if df.empty:
        st.info("No geographic breakdown yet.", icon="🌍")
        return
    c1, c2 = st.columns(2, gap="medium")
    with c1:
        fig = px.pie(df, values="revenue", names="country", hole=0.55,
                     color_discrete_sequence=px.colors.sequential.Teal)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        apply_plotly_theme(fig, height=340)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        fig = px.bar(df.sort_values("orders"), x="orders", y="country", orientation="h",
                     color="revenue", color_continuous_scale=["#312e81", COLORS["accent_secondary"]])
        fig.update_layout(xaxis_title="Orders", yaxis_title="")
        apply_plotly_theme(fig, height=340)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)


def render_clickstream(df) -> None:
    st.markdown('<div class="chart-panel"><p class="chart-panel-title">Clickstream (Recent)</p>', unsafe_allow_html=True)
    if df.empty:
        st.caption("No clickstream metrics.")
        return
    display = df.tail(6)[["window_start", "page_views", "unique_sessions", "top_page"]].copy()
    display.columns = ["Window", "Page Views", "Sessions", "Top Page"]
    st.dataframe(display, use_container_width=True, hide_index=True, height=220)
    st.markdown("</div>", unsafe_allow_html=True)


def render_sidebar(live: bool) -> tuple:
    with st.sidebar:
        st.markdown(f"### ⚙️ Controls")
        auto_refresh = st.toggle("Live refresh", value=True, help="Poll metrics every few seconds")
        time_window = st.selectbox(
            "Chart time window",
            options=[15, 30, 60, 120],
            index=2,
            format_func=lambda x: f"Last {x} minutes",
        )
        st.markdown("---")
        st.markdown("### Pipeline health")
        services = [
            ("Kafka", True, "Ingesting ~15 evt/sec"),
            ("Spark Streaming", live, "Unified structured job"),
            ("PostgreSQL", True, "Metrics + snapshot"),
            ("Data Lake", True, "Bronze parquet local"),
            ("CDC / Debezium", False, "Optional profile"),
        ]
        for name, ok, hint in services:
            icon = "🟢" if ok else "🟡"
            st.markdown(f"{icon} **{name}**  \n<span style='color:#8b9cb3;font-size:0.75rem'>{hint}</span>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown(
            f"<span style='color:#8b9cb3;font-size:0.75rem'>"
            f"Medallion: bronze → silver → gold<br/>"
            f"Batch: Glue-style ETL → warehouse</span>",
            unsafe_allow_html=True,
        )
    return auto_refresh, time_window


def main():
    inject_global_css()
    auto_refresh, time_window = render_sidebar(live=has_live_data())

    with st.spinner("Loading metrics…"):
        data = load_metrics(time_window)

    kpis = data["kpis"]
    live = data["live"]

    render_brand_header()
    render_stream_banner(live, kpis.get("snapshot_time"), auto_refresh)

    if not live:
        st.warning(
            "Pipeline warming up — display may show seeded or partial data. "
            "Live path: Kafka → Spark → PostgreSQL.",
            icon="⏳",
        )

    render_kpi_row(kpis)
    section_divider()

    row1_l, row1_r = st.columns([2.1, 1], gap="large")
    with row1_l:
        render_revenue_chart(data["revenue_ts"])
    with row1_r:
        render_users_chart(data["users_ts"])
        render_clickstream(data["clicks"])

    section_divider()

    row2_l, row2_r = st.columns(2, gap="large")
    with row2_l:
        render_top_products(data["top_products"])
    with row2_r:
        render_geo_charts(data["geo"])

    section_divider()
    st.caption(
        "CommercePulse · Kafka → Spark Structured Streaming → Parquet Lake + PostgreSQL · "
        "Portfolio data engineering platform"
    )

    if auto_refresh:
        time.sleep(REFRESH_SECONDS)
        st.rerun()


if __name__ == "__main__":
    main()
