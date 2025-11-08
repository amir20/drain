"""Visualization components for the retention analysis dashboard."""

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from config import (
    ENGAGEMENT_DETAILS_TAIL,
    HEATMAP_COLOR_SCALE,
    HEATMAP_GAP,
    HEATMAP_HEIGHT,
    HEATMAP_WIDTH,
    LIFECYCLE_DETAILS_TAIL,
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

    st.plotly_chart(fig_go, width="stretch")


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
        st.plotly_chart(fig_usage, width="stretch")

    # Detailed table
    with st.expander("Show Usage Frequency Details"):
        st.dataframe(
            usage_frequency.select(
                ["week_date", "avg_events_per_user_per_week", "active_users"]
            ).tail(USAGE_DETAILS_TAIL)
        )


def display_user_lifecycle_analysis(lifecycle_df: pl.DataFrame) -> None:
    """Display user lifecycle analysis section.

    Args:
        lifecycle_df: DataFrame with lifecycle metrics (new, retained, churned, resurrected users).
    """
    st.header("User Lifecycle Analysis")

    # Recent metrics
    recent_week = lifecycle_df.tail(1)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("New Users (Last Week)", f"{recent_week['new_users'][0]:,}")

    with col2:
        st.metric("Retained Users (Last Week)", f"{recent_week['retained_users'][0]:,}")

    with col3:
        st.metric("Churned Users (Last Week)", f"{recent_week['churned_users'][0]:,}")

    with col4:
        st.metric(
            "Resurrected Users (Last Week)", f"{recent_week['resurrected_users'][0]:,}"
        )

    # Lifecycle stacked area chart
    with st.spinner("Generating lifecycle chart..."):
        lifecycle_pd = lifecycle_df.to_pandas()

        fig_lifecycle = go.Figure()

        fig_lifecycle.add_trace(
            go.Scatter(
                x=lifecycle_pd["week_date"],
                y=lifecycle_pd["new_users"],
                mode="lines",
                name="New Users",
                stackgroup="one",
                fillcolor="rgba(99, 110, 250, 0.5)",
            )
        )

        fig_lifecycle.add_trace(
            go.Scatter(
                x=lifecycle_pd["week_date"],
                y=lifecycle_pd["retained_users"],
                mode="lines",
                name="Retained Users",
                stackgroup="one",
                fillcolor="rgba(0, 204, 150, 0.5)",
            )
        )

        fig_lifecycle.add_trace(
            go.Scatter(
                x=lifecycle_pd["week_date"],
                y=lifecycle_pd["resurrected_users"],
                mode="lines",
                name="Resurrected Users",
                stackgroup="one",
                fillcolor="rgba(255, 161, 90, 0.5)",
            )
        )

        fig_lifecycle.update_layout(
            title="User Lifecycle Over Time",
            xaxis_title="Week",
            yaxis_title="Number of Users",
            hovermode="x unified",
        )

        st.plotly_chart(fig_lifecycle, width="stretch")

    # Churned users chart
    with st.spinner("Generating churn chart..."):
        fig_churn = px.line(
            lifecycle_pd,
            x="week_date",
            y="churned_users",
            title="Churned Users Over Time",
            labels={"week_date": "Week", "churned_users": "Churned Users"},
        )
        fig_churn.update_traces(mode="lines+markers", line_color="red")
        st.plotly_chart(fig_churn, width="stretch")

    # Detailed table
    with st.expander("Show Lifecycle Details"):
        st.dataframe(
            lifecycle_df.select(
                [
                    "week_date",
                    "new_users",
                    "retained_users",
                    "churned_users",
                    "resurrected_users",
                    "total_active_users",
                ]
            ).tail(LIFECYCLE_DETAILS_TAIL)
        )


def display_stickiness_analysis(
    stickiness_df: pl.DataFrame, summary_stats: dict
) -> None:
    """Display stickiness analysis section.

    Args:
        stickiness_df: DataFrame with stickiness metrics.
        summary_stats: Dictionary with summary statistics.
    """
    st.header("User Stickiness Analysis")

    # Metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Average Stickiness Ratio", f"{summary_stats['avg_stickiness']:.2%}")

    with col2:
        st.metric("Current WAU", f"{summary_stats['current_wau']:,}")

    with col3:
        st.metric("Current MAU (4-week)", f"{summary_stats['current_mau']:,}")

    # Stickiness chart
    with st.spinner("Generating stickiness chart..."):
        fig_stickiness = px.line(
            stickiness_df.to_pandas(),
            x="week_date",
            y="stickiness_ratio",
            title="User Stickiness Over Time (WAU / MAU)",
            labels={
                "week_date": "Week",
                "stickiness_ratio": "Stickiness Ratio (WAU/MAU)",
            },
        )
        fig_stickiness.update_traces(mode="lines+markers")
        fig_stickiness.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig_stickiness, width="stretch")

    # WAU vs MAU chart
    with st.spinner("Generating WAU/MAU chart..."):
        fig_wau_mau = go.Figure()

        fig_wau_mau.add_trace(
            go.Scatter(
                x=stickiness_df["week_date"],
                y=stickiness_df["wau"],
                mode="lines+markers",
                name="WAU",
                line=dict(color="blue"),
            )
        )

        fig_wau_mau.add_trace(
            go.Scatter(
                x=stickiness_df["week_date"],
                y=stickiness_df["mau_rolling_max"],
                mode="lines+markers",
                name="MAU (4-week rolling)",
                line=dict(color="green"),
            )
        )

        fig_wau_mau.update_layout(
            title="Weekly Active Users (WAU) vs Monthly Active Users (MAU)",
            xaxis_title="Week",
            yaxis_title="Number of Users",
            hovermode="x unified",
        )

        st.plotly_chart(fig_wau_mau, width="stretch")


def display_engagement_depth_analysis(engagement_df: pl.DataFrame) -> None:
    """Display engagement depth analysis section.

    Args:
        engagement_df: DataFrame with engagement depth distribution.
    """
    st.header("Engagement Depth Analysis")

    # Create stacked area chart
    with st.spinner("Generating engagement depth chart..."):
        # Pivot data for stacking
        engagement_pivot = engagement_df.pivot(
            on="engagement_level",
            index="week_date",
            values="user_count",
            aggregate_function="first",
        ).fill_null(0)

        engagement_pd = engagement_pivot.to_pandas().set_index("week_date")

        # Define order for engagement levels
        level_order = [
            "1_event",
            "2-5_events",
            "6-20_events",
            "21-50_events",
            "50+_events",
        ]
        engagement_pd = engagement_pd[
            [col for col in level_order if col in engagement_pd.columns]
        ]

        fig_engagement = go.Figure()

        colors = {
            "1_event": "rgba(239, 85, 59, 0.6)",
            "2-5_events": "rgba(255, 161, 90, 0.6)",
            "6-20_events": "rgba(255, 217, 102, 0.6)",
            "21-50_events": "rgba(99, 190, 123, 0.6)",
            "50+_events": "rgba(0, 204, 150, 0.6)",
        }

        for level in engagement_pd.columns:
            fig_engagement.add_trace(
                go.Scatter(
                    x=engagement_pd.index,
                    y=engagement_pd[level],
                    mode="lines",
                    name=level.replace("_", " ").title(),
                    stackgroup="one",
                    fillcolor=colors.get(level, "rgba(99, 110, 250, 0.5)"),
                )
            )

        fig_engagement.update_layout(
            title="User Engagement Depth Over Time",
            xaxis_title="Week",
            yaxis_title="Number of Users",
            hovermode="x unified",
        )

        st.plotly_chart(fig_engagement, width="stretch")

    # Detailed table
    with st.expander("Show Engagement Depth Details"):
        st.dataframe(
            engagement_df.select(["week_date", "engagement_level", "user_count"]).tail(
                ENGAGEMENT_DETAILS_TAIL
            )
        )


def display_cohort_engagement_analysis(cohort_engagement_df: pl.DataFrame) -> None:
    """Display cohort engagement analysis section.

    Args:
        cohort_engagement_df: DataFrame with engagement metrics by cohort age.
    """
    st.header("Cohort Engagement by Age")

    # Engagement by cohort age chart
    with st.spinner("Generating cohort engagement chart..."):
        # Limit to first 20 weeks for readability
        cohort_engagement_limited = cohort_engagement_df.filter(
            pl.col("cohort_index") <= 20
        )

        fig_cohort_engagement = px.bar(
            cohort_engagement_limited.to_pandas(),
            x="cohort_index",
            y="avg_events_per_user",
            title="Average Events per User by Cohort Age",
            labels={
                "cohort_index": "Weeks Since Activation",
                "avg_events_per_user": "Avg Events per User",
            },
        )
        st.plotly_chart(fig_cohort_engagement, width="stretch")

    # Show summary statistics
    col1, col2 = st.columns(2)

    with col1:
        week_0_engagement = cohort_engagement_df.filter(pl.col("cohort_index") == 0)
        if len(week_0_engagement) > 0:
            st.metric(
                "Week 0 Avg Events",
                f"{week_0_engagement['avg_events_per_user'][0]:.2f}",
            )

    with col2:
        week_4_engagement = cohort_engagement_df.filter(pl.col("cohort_index") == 4)
        if len(week_4_engagement) > 0:
            st.metric(
                "Week 4 Avg Events",
                f"{week_4_engagement['avg_events_per_user'][0]:.2f}",
            )
