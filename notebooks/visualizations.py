"""Visualization components for the retention analysis dashboard."""

import polars as pl
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from config import (
    HEATMAP_HEIGHT,
    HEATMAP_WIDTH,
    HEATMAP_COLOR_SCALE,
    HEATMAP_GAP,
    RECENT_WEEKS_COUNT,
    USAGE_DETAILS_TAIL,
)


def display_retention_heatmap(retention: pl.DataFrame) -> None:
    """Display cohort retention heatmap.

    Args:
        retention: Retention matrix dataframe.
    """
    retention_pd = retention.to_pandas()

    if "activated_date" in retention_pd.columns:
        retention_pd = retention_pd.set_index("activated_date")

    fig = px.imshow(
        retention_pd,
        labels=dict(x="Week", y="Cohort", color="Retention Rate"),
        x=retention_pd.columns,
        y=retention_pd.index,
        color_continuous_scale=HEATMAP_COLOR_SCALE,
        zmin=0,
        zmax=1,
        aspect="auto",
        text_auto=".0%",
    )

    fig_go = go.Figure(fig)
    fig_go.data[0].xgap = HEATMAP_GAP
    fig_go.data[0].ygap = HEATMAP_GAP

    fig_go.update_layout(
        title="Cohort Retention Analysis",
        xaxis_title="Week Number",
        yaxis_title="Cohort Start Date",
        coloraxis_colorbar=dict(title="Retention Rate"),
        height=HEATMAP_HEIGHT,
        width=HEATMAP_WIDTH,
    )

    st.plotly_chart(fig_go, use_container_width=True)


def display_usage_frequency_analysis(
    usage_frequency: pl.DataFrame, overall_avg: pl.DataFrame
) -> None:
    """Display usage frequency analysis section.

    Args:
        usage_frequency: Weekly usage frequency metrics.
        overall_avg: Overall average events per user per week.
    """
    st.header("Usage Frequency Analysis")

    # Metrics
    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Overall Average Events per User per Week",
            f"{overall_avg['overall_avg_events_per_user_per_week'][0]:.2f}",
        )

    with col2:
        recent_avg = usage_frequency.tail(RECENT_WEEKS_COUNT).select(
            pl.col("avg_events_per_user_per_week").mean()
        )[0, 0]
        st.metric(
            f"Recent Average (Last {RECENT_WEEKS_COUNT} Weeks)", f"{recent_avg:.2f}"
        )

    # Usage frequency chart
    with st.spinner("Generating usage frequency chart..."):
        fig_usage = px.line(
            usage_frequency.to_pandas(),
            x="week_date",
            y="avg_events_per_user_per_week",
            title="Average Events per User per Week Over Time",
            labels={
                "week_date": "Week",
                "avg_events_per_user_per_week": "Avg Events per User",
            },
        )
        fig_usage.update_traces(mode="lines+markers")
        st.plotly_chart(fig_usage, use_container_width=True)

    # Detailed table
    with st.expander("Show Usage Frequency Details"):
        st.dataframe(
            usage_frequency.select(
                ["week_date", "avg_events_per_user_per_week", "active_users"]
            ).tail(USAGE_DETAILS_TAIL)
        )
