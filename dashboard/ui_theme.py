"""
Executive dashboard theme — Datadog / Grafana / Snowflake inspired dark UI.
"""

import streamlit as st

# Brand
BRAND_NAME = "CommercePulse"
BRAND_TAGLINE = "Real-Time E-Commerce Intelligence"

# Palette
COLORS = {
    "bg": "#0b0f14",
    "surface": "#131a24",
    "surface_elevated": "#1a2332",
    "border": "#2d3a4f",
    "text": "#e8edf4",
    "text_muted": "#8b9cb3",
    "accent": "#22d3ee",
    "accent_secondary": "#6366f1",
    "success": "#34d399",
    "warning": "#fbbf24",
    "danger": "#f87171",
    "chart_grid": "#1e293b",
}

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, system-ui, sans-serif", color=COLORS["text"], size=12),
    margin=dict(l=48, r=24, t=48, b=40),
    height=380,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=11)),
    xaxis=dict(gridcolor=COLORS["chart_grid"], zeroline=False, showline=False),
    yaxis=dict(gridcolor=COLORS["chart_grid"], zeroline=False, showline=False),
    hovermode="x unified",
)


def inject_global_css() -> None:
    c = COLORS
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

        .stApp {{
            background: linear-gradient(165deg, {c['bg']} 0%, #0f1419 45%, #0b1020 100%);
            font-family: 'Inter', system-ui, sans-serif;
        }}

        [data-testid="stSidebar"] {{
            background: {c['surface']};
            border-right: 1px solid {c['border']};
        }}

        [data-testid="stSidebar"] .stMarkdown h1,
        [data-testid="stSidebar"] .stMarkdown h2,
        [data-testid="stSidebar"] .stMarkdown h3 {{
            color: {c['text']};
            font-weight: 600;
        }}

        h1, h2, h3, p, label, span {{
            font-family: 'Inter', system-ui, sans-serif !important;
        }}

        div[data-testid="stMetricValue"] {{
            font-size: 1.75rem !important;
            font-weight: 700 !important;
            color: {c['text']} !important;
        }}

        div[data-testid="stMetricLabel"] {{
            font-size: 0.8rem !important;
            font-weight: 500 !important;
            color: {c['text_muted']} !important;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }}

        .section-divider {{
            height: 1px;
            background: linear-gradient(90deg, transparent, {c['border']}, transparent);
            margin: 1.5rem 0;
        }}

        .brand-header {{
            display: flex;
            align-items: center;
            gap: 14px;
            margin-bottom: 0.25rem;
        }}

        .brand-logo {{
            width: 44px;
            height: 44px;
            border-radius: 10px;
            background: linear-gradient(135deg, {c['accent']} 0%, {c['accent_secondary']} 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 800;
            font-size: 1.1rem;
            color: #0b0f14;
            box-shadow: 0 4px 20px rgba(34, 211, 238, 0.25);
        }}

        .brand-title {{
            font-size: 1.65rem;
            font-weight: 700;
            color: {c['text']};
            letter-spacing: -0.02em;
            margin: 0;
            line-height: 1.2;
        }}

        .brand-subtitle {{
            font-size: 0.85rem;
            color: {c['text_muted']};
            margin: 0;
        }}

        .kpi-card {{
            background: {c['surface_elevated']};
            border: 1px solid {c['border']};
            border-radius: 12px;
            padding: 1.1rem 1.25rem;
            min-height: 108px;
            transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
            animation: kpiFadeIn 0.5s ease-out forwards;
            opacity: 0;
        }}

        .kpi-card:hover {{
            transform: translateY(-2px);
            border-color: {c['accent']}55;
            box-shadow: 0 8px 28px rgba(0, 0, 0, 0.35);
        }}

        @keyframes kpiFadeIn {{
            from {{ opacity: 0; transform: translateY(8px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        .kpi-card.delay-1 {{ animation-delay: 0.05s; }}
        .kpi-card.delay-2 {{ animation-delay: 0.1s; }}
        .kpi-card.delay-3 {{ animation-delay: 0.15s; }}
        .kpi-card.delay-4 {{ animation-delay: 0.2s; }}
        .kpi-card.delay-5 {{ animation-delay: 0.25s; }}

        .kpi-label {{
            font-size: 0.72rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: {c['text_muted']};
            margin-bottom: 0.35rem;
        }}

        .kpi-value {{
            font-size: 1.65rem;
            font-weight: 700;
            color: {c['text']};
            line-height: 1.2;
        }}

        .kpi-value.accent {{ color: {c['accent']}; }}
        .kpi-value.success {{ color: {c['success']}; }}
        .kpi-value.danger {{ color: {c['danger']}; }}

        .status-pill {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 600;
        }}

        .status-live {{
            background: rgba(52, 211, 153, 0.15);
            color: {c['success']};
            border: 1px solid rgba(52, 211, 153, 0.35);
        }}

        .status-warn {{
            background: rgba(251, 191, 36, 0.12);
            color: {c['warning']};
            border: 1px solid rgba(251, 191, 36, 0.3);
        }}

        .status-dot {{
            width: 7px;
            height: 7px;
            border-radius: 50%;
            background: currentColor;
        }}

        .status-dot.pulse {{
            animation: pulse 1.5s ease-in-out infinite;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.5; transform: scale(0.85); }}
        }}

        .stream-banner {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 12px;
            background: {c['surface_elevated']};
            border: 1px solid {c['border']};
            border-radius: 10px;
            padding: 0.65rem 1rem;
            margin-bottom: 1.25rem;
        }}

        .chart-panel {{
            background: {c['surface_elevated']};
            border: 1px solid {c['border']};
            border-radius: 12px;
            padding: 0.5rem 0.75rem 0.25rem;
            margin-bottom: 0.5rem;
        }}

        .chart-panel-title {{
            font-size: 0.95rem;
            font-weight: 600;
            color: {c['text']};
            padding: 0.5rem 0.75rem 0;
            margin: 0;
        }}

        .skeleton {{
            background: linear-gradient(90deg, {c['surface']} 25%, {c['border']} 50%, {c['surface']} 75%);
            background-size: 200% 100%;
            animation: shimmer 1.2s infinite;
            border-radius: 8px;
            height: 320px;
        }}

        @keyframes shimmer {{
            0% {{ background-position: 200% 0; }}
            100% {{ background-position: -200% 0; }}
        }}

        @media (max-width: 768px) {{
            .brand-title {{ font-size: 1.35rem; }}
            .kpi-value {{ font-size: 1.35rem; }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card_html(
    label: str,
    value: str,
    delay_class: str = "",
    value_class: str = "",
    tooltip: str = "",
) -> str:
    tip = f' title="{tooltip}"' if tooltip else ""
    return f"""
    <div class="kpi-card {delay_class}"{tip}>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value {value_class}">{value}</div>
    </div>
    """


def status_pill(label: str, status: str = "live", pulse: bool = False) -> str:
    cls = "status-live" if status == "live" else "status-warn"
    dot = "pulse" if pulse else ""
    return f'<span class="status-pill {cls}"><span class="status-dot {dot}"></span>{label}</span>'


def section_divider() -> None:
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)


def chart_panel(title: str):
    """Context manager helper via class."""
    st.markdown(f'<div class="chart-panel"><p class="chart-panel-title">{title}</p>', unsafe_allow_html=True)
    return st


def apply_plotly_theme(fig, height: int = 380):
    fig.update_layout(**{**PLOTLY_LAYOUT, "height": height})
    return fig
