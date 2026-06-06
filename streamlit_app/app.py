"""Interactive Wistia engagement dashboard for Streamlit Community Cloud."""

from __future__ import annotations

import os
from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from data_access import DashboardConfig, load_dashboard_data
from demo_data import load_demo_data


st.set_page_config(
    page_title="Wistia Engagement",
    page_icon="▶",
    layout="wide",
    initial_sidebar_state="auto",
)

BRAND_BLUE = "#2563EB"
BRAND_CYAN = "#06B6D4"
BRAND_ORANGE = "#F97316"
MUTED = "#64748B"


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at 8% 0%, rgba(37, 99, 235, 0.10), transparent 28rem),
                radial-gradient(circle at 96% 8%, rgba(6, 182, 212, 0.08), transparent 24rem),
                #f8fafc;
        }
        [data-testid="stSidebar"] {
            background: #0f172a;
        }
        [data-testid="stSidebar"] * {
            color: #e2e8f0;
        }
        [data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
        }
        [data-testid="stMetricLabel"] {
            color: #64748b;
        }
        .hero {
            padding: 1.4rem 1.6rem;
            border-radius: 20px;
            color: white;
            background: linear-gradient(120deg, #0f172a 0%, #1e3a8a 55%, #0891b2 100%);
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18);
            margin-bottom: 1.2rem;
        }
        .hero h1 {
            margin: 0;
            font-size: clamp(2rem, 4vw, 3.2rem);
            letter-spacing: -0.04em;
        }
        .hero p {
            margin: 0.4rem 0 0;
            color: #cbd5e1;
            font-size: 1rem;
        }
        .section-label {
            color: #475569;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-top: 0.8rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_percent(value: float) -> str:
    return f"{value:.1%}"


def format_integer(value: int | float) -> str:
    return f"{int(value):,}"


def apply_filters(
    frame: pd.DataFrame,
    media_ids: list[str],
    channels: list[str],
    countries: list[str],
    date_range: tuple[date, date],
) -> pd.DataFrame:
    filtered = frame[
        frame["media_id"].isin(media_ids)
        & frame["channel"].isin(channels)
        & frame["country"].isin(countries)
    ]
    start_date, end_date = date_range
    return filtered[
        (filtered["last_date_watched"].dt.date >= start_date)
        & (filtered["first_date_watched"].dt.date <= end_date)
    ].copy()


def render_empty_state() -> None:
    st.warning("No engagement records match the selected filters.")
    st.stop()


inject_styles()

demo_mode = os.environ.get("WISTIA_DASHBOARD_DEMO_MODE", "").lower() == "true"
if not demo_mode:
    try:
        config = DashboardConfig.from_secrets(st.secrets)
    except (KeyError, ValueError) as exc:
        st.error("The dashboard is not configured yet.")
        st.code(str(exc))
        st.stop()

with st.sidebar:
    st.markdown("## Wistia Analytics")
    st.caption("Visitor engagement explorer")
    if demo_mode:
        st.info("Synthetic demo data")
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

try:
    if demo_mode:
        engagement, metadata = load_demo_data()
    else:
        engagement, metadata = load_dashboard_data(config)
except Exception as exc:
    st.error("The dashboard could not load its Delta tables.")
    st.exception(exc)
    st.stop()

if engagement.empty:
    st.info("The curated visitor_engagement table is empty.")
    st.stop()

all_media = sorted(engagement["media_id"].dropna().unique().tolist())
all_channels = sorted(engagement["channel"].dropna().unique().tolist())
all_countries = sorted(engagement["country"].dropna().unique().tolist())
min_date = engagement["first_date_watched"].min().date()
max_date = engagement["last_date_watched"].max().date()

with st.sidebar:
    st.markdown("### Filters")
    selected_media = st.multiselect(
        "Media",
        all_media,
        default=all_media,
        format_func=lambda media_id: metadata["media_labels"].get(media_id, media_id),
    )
    selected_channels = st.multiselect(
        "Channel",
        all_channels,
        default=all_channels,
    )
    selected_countries = st.multiselect(
        "Country",
        all_countries,
        default=all_countries,
    )
    selected_dates = st.date_input(
        "Watch-date overlap",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

if len(selected_dates) != 2:
    st.info("Select both a start and end date.")
    st.stop()

filtered = apply_filters(
    engagement,
    selected_media,
    selected_channels,
    selected_countries,
    (selected_dates[0], selected_dates[1]),
)
if filtered.empty:
    render_empty_state()

st.markdown(
    """
    <div class="hero">
      <h1>Visitor Engagement</h1>
      <p>Two-year video performance across audiences, channels, and media.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

unique_visitors = filtered["visitor_id"].nunique()
total_views = filtered["total_views"].sum()
weighted_average = (
    (filtered["avg_pct_viewed"] * filtered["total_views"]).sum() / total_views
    if total_views
    else 0.0
)
high_intent_visitors = filtered.loc[
    filtered["max_pct_viewed"] >= 0.75, "visitor_id"
].nunique()

kpi_columns = st.columns(4)
kpi_columns[0].metric("Unique visitors", format_integer(unique_visitors))
kpi_columns[1].metric("Total views", format_integer(total_views))
kpi_columns[2].metric("Avg. viewed", format_percent(weighted_average))
kpi_columns[3].metric("75%+ viewers", format_integer(high_intent_visitors))

st.markdown('<div class="section-label">Performance overview</div>', unsafe_allow_html=True)
left, right = st.columns((1.4, 1), gap="large")

media_summary = (
    filtered.groupby(["media_id", "title", "channel"], as_index=False)
    .agg(
        total_views=("total_views", "sum"),
        unique_visitors=("visitor_id", "nunique"),
    )
)
weighted_by_media = (
    filtered.assign(
        weighted_views=filtered["avg_pct_viewed"] * filtered["total_views"]
    )
    .groupby("media_id")
    .agg(weighted_views=("weighted_views", "sum"), views=("total_views", "sum"))
)
media_summary["avg_pct_viewed"] = media_summary["media_id"].map(
    (weighted_by_media["weighted_views"] / weighted_by_media["views"]).to_dict()
)

with left:
    views_chart = (
        alt.Chart(media_summary)
        .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
        .encode(
            y=alt.Y("title:N", title=None, sort="-x"),
            x=alt.X("total_views:Q", title="Total views"),
            color=alt.Color(
                "channel:N",
                scale=alt.Scale(
                    domain=["Youtube", "Facebook"],
                    range=[BRAND_BLUE, BRAND_CYAN],
                ),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("title:N", title="Media"),
                alt.Tooltip("channel:N", title="Channel"),
                alt.Tooltip("total_views:Q", title="Views", format=","),
                alt.Tooltip("unique_visitors:Q", title="Visitors", format=","),
                alt.Tooltip("avg_pct_viewed:Q", title="Avg. viewed", format=".1%"),
            ],
        )
        .properties(height=300, title="Views by media")
    )
    st.altair_chart(views_chart, use_container_width=True)

with right:
    country_summary = (
        filtered.groupby("country", as_index=False)
        .agg(total_views=("total_views", "sum"))
        .sort_values("total_views", ascending=False)
        .head(10)
    )
    country_chart = (
        alt.Chart(country_summary)
        .mark_bar(color=BRAND_ORANGE, cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
        .encode(
            x=alt.X("country:N", title=None, sort="-y"),
            y=alt.Y("total_views:Q", title="Total views"),
            tooltip=[
                alt.Tooltip("country:N", title="Country"),
                alt.Tooltip("total_views:Q", title="Views", format=","),
            ],
        )
        .properties(height=300, title="Top countries")
    )
    st.altair_chart(country_chart, use_container_width=True)

st.markdown('<div class="section-label">Audience quality</div>', unsafe_allow_html=True)
left, right = st.columns((1, 1.25), gap="large")

with left:
    engagement_bands = pd.cut(
        filtered["avg_pct_viewed"],
        bins=[-0.001, 0.25, 0.5, 0.75, 1.0],
        labels=["0–25%", "25–50%", "50–75%", "75–100%"],
    )
    band_summary = (
        filtered.assign(engagement_band=engagement_bands)
        .groupby("engagement_band", observed=True, as_index=False)
        .agg(visitor_media_pairs=("visitor_id", "size"))
    )
    band_chart = (
        alt.Chart(band_summary)
        .mark_arc(innerRadius=62, outerRadius=105)
        .encode(
            theta=alt.Theta("visitor_media_pairs:Q"),
            color=alt.Color(
                "engagement_band:N",
                title=None,
                scale=alt.Scale(
                    domain=["0–25%", "25–50%", "50–75%", "75–100%"],
                    range=["#cbd5e1", "#93c5fd", "#38bdf8", "#1d4ed8"],
                ),
            ),
            tooltip=[
                alt.Tooltip("engagement_band:N", title="Avg. engagement"),
                alt.Tooltip(
                    "visitor_media_pairs:Q",
                    title="Visitor-media pairs",
                    format=",",
                ),
            ],
        )
        .properties(height=330, title="Average engagement distribution")
    )
    st.altair_chart(band_chart, use_container_width=True)

with right:
    top_visitors = (
        filtered.groupby(["visitor_id", "country"], as_index=False)
        .agg(
            total_views=("total_views", "sum"),
            media_watched=("media_id", "nunique"),
            max_pct_viewed=("max_pct_viewed", "max"),
            last_date_watched=("last_date_watched", "max"),
        )
        .sort_values(["total_views", "max_pct_viewed"], ascending=False)
        .head(15)
    )
    st.dataframe(
        top_visitors,
        use_container_width=True,
        hide_index=True,
        column_config={
            "visitor_id": "Visitor",
            "country": "Country",
            "total_views": st.column_config.NumberColumn("Views", format="%d"),
            "media_watched": st.column_config.NumberColumn("Media", format="%d"),
            "max_pct_viewed": st.column_config.ProgressColumn(
                "Max viewed",
                min_value=0.0,
                max_value=1.0,
                format="percent",
            ),
            "last_date_watched": st.column_config.DateColumn("Last watched"),
        },
        height=330,
    )

with st.expander("Explore visitor-media detail"):
    detail = filtered[
        [
            "visitor_id",
            "media_id",
            "title",
            "channel",
            "country",
            "ip_address",
            "total_views",
            "avg_pct_viewed",
            "max_pct_viewed",
            "first_date_watched",
            "last_date_watched",
        ]
    ].sort_values(["total_views", "avg_pct_viewed"], ascending=False)
    st.dataframe(
        detail,
        use_container_width=True,
        hide_index=True,
        column_config={
            "avg_pct_viewed": st.column_config.NumberColumn(
                "Avg. viewed", format="percent"
            ),
            "max_pct_viewed": st.column_config.NumberColumn(
                "Max viewed", format="percent"
            ),
            "first_date_watched": st.column_config.DateColumn("First watched"),
            "last_date_watched": st.column_config.DateColumn("Last watched"),
        },
    )
    st.download_button(
        "Download filtered CSV",
        detail.to_csv(index=False).encode("utf-8"),
        file_name="visitor_engagement.csv",
        mime="text/csv",
    )

st.caption(
    f"Delta version {metadata['curated_version']} · "
    f"Loaded {metadata['loaded_at']} · Percentages use Wistia's 0–1 scale."
)
