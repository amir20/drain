"""
Visualizations module for the Dozzle Analytics Dashboard.

Contains all display, plotting, and UI functions.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from typing import Dict
from .config import (
    CUSTOM_CSS,
    CHART_COLORS,
    WEEKDAY_MAP,
)


def apply_custom_styling():
    """Apply custom CSS styling to the dashboard."""
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def display_header():
    """Display the main dashboard header."""
    st.markdown(
        '<div class="main-header">📊 Dozzle Analytics Dashboard</div>',
        unsafe_allow_html=True,
    )


def display_key_metrics(growth_metrics: Dict):
    """Display key performance indicators."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Users",
            f"{growth_metrics['total_users']:,}",
            help="Total unique users in the dataset",
        )

    with col2:
        st.metric(
            "Total Events",
            f"{growth_metrics['total_events']:,}",
            help="Total events recorded",
        )

    with col3:
        avg_events = growth_metrics["avg_events_per_user"]
        st.metric(
            "Avg Events/User", f"{avg_events:.1f}", help="Average events per user"
        )

    with col4:
        start_date, end_date = growth_metrics["date_range"]
        days_range = (end_date - start_date).days
        st.metric(
            "Data Range",
            f"{days_range} days",
            help=f"From {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        )


def display_growth_analysis(growth_metrics: Dict):
    """Display growth analysis section."""
    st.header("📈 Growth Analysis")

    weekly_stats = growth_metrics["weekly_stats"]

    # Growth trends
    fig_growth = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Active Users Over Time",
            "New vs Returning Users",
            "User Growth Rate",
            "Event Volume",
        ),
        specs=[
            [{"secondary_y": False}, {"type": "bar"}],
            [{"secondary_y": True}, {"secondary_y": False}],
        ],
    )

    # Active users trend
    fig_growth.add_trace(
        go.Scatter(
            x=weekly_stats["week_date"].to_pandas(),
            y=weekly_stats["active_users"].to_pandas(),
            mode="lines+markers",
            name="Active Users",
            line=dict(color="#3b82f6", width=3),
        ),
        row=1,
        col=1,
    )

    # New vs returning users stacked bar
    fig_growth.add_trace(
        go.Bar(
            x=weekly_stats["week_date"].to_pandas(),
            y=weekly_stats["new_users"].to_pandas(),
            name="New Users",
            marker_color="#10b981",
        ),
        row=1,
        col=2,
    )

    fig_growth.add_trace(
        go.Bar(
            x=weekly_stats["week_date"].to_pandas(),
            y=weekly_stats["returning_users"].to_pandas(),
            name="Returning Users",
            marker_color="#6366f1",
        ),
        row=1,
        col=2,
    )

    # Growth rate
    fig_growth.add_trace(
        go.Scatter(
            x=weekly_stats["week_date"].to_pandas(),
            y=weekly_stats["new_user_growth_rate"].to_pandas(),
            mode="lines+markers",
            name="New User Growth Rate (%)",
            line=dict(color="#ef4444", width=2),
        ),
        row=2,
        col=1,
    )

    # Event volume
    fig_growth.add_trace(
        go.Scatter(
            x=weekly_stats["week_date"].to_pandas(),
            y=weekly_stats["total_events"].to_pandas(),
            mode="lines+markers",
            name="Total Events",
            line=dict(color="#f59e0b", width=2),
            fill="tonexty",
        ),
        row=2,
        col=2,
    )

    fig_growth.update_layout(
        height=600, showlegend=True, title_text="Growth Metrics Overview", title_x=0.5
    )

    fig_growth.update_xaxes(title_text="Date")
    fig_growth.update_yaxes(title_text="Users", row=1, col=1)
    fig_growth.update_yaxes(title_text="Users", row=1, col=2)
    fig_growth.update_yaxes(title_text="Growth Rate (%)", row=2, col=1)
    fig_growth.update_yaxes(title_text="Events", row=2, col=2)

    st.plotly_chart(fig_growth, use_container_width=True)


def display_engagement_analysis(engagement_metrics: Dict):
    """Display user engagement analysis."""
    st.header("🎯 Engagement Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Hourly activity heatmap
        daily_activity = engagement_metrics["daily_activity"]

        fig_hourly = px.bar(
            daily_activity.to_pandas(),
            x="hour_of_day",
            y="unique_users",
            title="User Activity by Hour of Day",
            color="unique_users",
            color_continuous_scale=CHART_COLORS["activity_bars"],
        )
        fig_hourly.update_layout(
            xaxis_title="Hour of Day", yaxis_title="Unique Users", showlegend=False
        )
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col2:
        # Day of week activity
        weekly_activity = engagement_metrics["weekly_activity"]

        weekly_df = weekly_activity.to_pandas()
        weekly_df["day_name"] = [
            WEEKDAY_MAP.get(i, f"Day {i}") for i in weekly_df["day_of_week"]
        ]

        fig_weekly = px.bar(
            weekly_df,
            x="day_name",
            y="unique_users",
            title="User Activity by Day of Week",
            color="unique_users",
            color_continuous_scale=CHART_COLORS["weekly_activity"],
        )
        fig_weekly.update_layout(
            xaxis_title="Day of Week", yaxis_title="Unique Users", showlegend=False
        )
        st.plotly_chart(fig_weekly, use_container_width=True)

    # User segments
    st.subheader("🎭 User Segmentation")

    segment_dist = engagement_metrics["segment_distribution"]

    col3, col4 = st.columns([1, 2])

    with col3:
        # Segment metrics
        for _, row in segment_dist.to_pandas().iterrows():
            st.metric(row["user_segment"], f"{row['user_count']:,} users")

    with col4:
        # Segment pie chart
        fig_segments = px.pie(
            segment_dist.to_pandas(),
            values="user_count",
            names="user_segment",
            title="User Distribution by Segment",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        st.plotly_chart(fig_segments, use_container_width=True)


def display_retention_heatmap(retention):
    """Display enhanced cohort retention heatmap."""
    st.header("🔄 Cohort Retention Analysis")

    retention_pd = retention.to_pandas()
    if "activated_date" in retention_pd.columns:
        retention_pd = retention_pd.set_index("activated_date")

    fig = px.imshow(
        retention_pd,
        labels=dict(x="Week", y="Cohort", color="Retention Rate"),
        x=retention_pd.columns,
        y=retention_pd.index,
        color_continuous_scale=CHART_COLORS["retention_heatmap"],
        zmin=0,
        zmax=1,
        aspect="auto",
        text_auto=True,
    )

    fig.update_layout(
        title={"text": "Cohort Retention Heatmap", "x": 0.5, "xanchor": "center"},
        xaxis_title="Week Number",
        yaxis_title="Cohort Start Date",
        coloraxis_colorbar=dict(title="Retention Rate"),
        height=600,
    )

    # Add gap between cells for better visualization
    fig.data[0].xgap = 2
    fig.data[0].ygap = 2

    st.plotly_chart(fig, use_container_width=True)

    # Retention insights
    numeric_cols = retention_pd.select_dtypes(include=[np.number])
    if not numeric_cols.empty:
        avg_retention = numeric_cols.mean().mean()

        # Safely get best performing cohort
        try:
            if len(retention_pd.columns) > 1 and len(retention_pd) > 0:
                week1_col = (
                    retention_pd.iloc[:, 1]
                    if retention_pd.shape[1] > 1
                    else retention_pd.iloc[:, 0]
                )
                best_cohort_idx = week1_col.idxmax()
                best_cohort = str(best_cohort_idx)
            else:
                best_cohort = "N/A"
        except (IndexError, KeyError, ValueError):
            best_cohort = "N/A"
    else:
        avg_retention = 0
        best_cohort = "N/A"

    st.markdown(
        f"""
    <div class="insight-box">
    <strong>💡 Key Insights:</strong><br>
    • Average retention rate across all cohorts: <strong>{avg_retention:.1%}</strong><br>
    • Best performing cohort: <strong>{best_cohort}</strong><br>
    • Week 1 retention typically indicates product-market fit strength
    </div>
    """,
        unsafe_allow_html=True,
    )


def display_churn_analysis(churn_metrics: Dict):
    """Display churn risk analysis."""
    st.header("🔬 Advanced Analytics")
    st.subheader("⚠️ Churn Risk Analysis")

    churn_pivot = churn_metrics["churn_pivot"]

    fig_churn = px.imshow(
        churn_pivot.to_pandas().set_index("churn_status"),
        title="User Churn Risk by Value Segment",
        color_continuous_scale=CHART_COLORS["churn_heatmap"],
        text_auto=True,
    )

    st.plotly_chart(fig_churn, use_container_width=True)


def display_sidebar():
    """Display sidebar controls and filters."""
    st.sidebar.title("🎛️ Dashboard Controls")
    st.sidebar.subheader("📅 Date Range")

    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto-refresh data", value=False)
    if auto_refresh:
        st.sidebar.write("🔄 Data refreshes every 5 minutes")

    return {"auto_refresh": auto_refresh}


def display_footer():
    """Display dashboard footer."""
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #6b7280;'>Built with ❤️ using Streamlit and Plotly</div>",
        unsafe_allow_html=True,
    )


def display_data_tables(growth_metrics: Dict, engagement_metrics: Dict, cohort_counts):
    """Display expandable data tables for detailed analysis."""
    # Create tabs for data tables
    with st.expander("📋 Raw Growth Data"):
        st.dataframe(growth_metrics["weekly_stats"].tail(20), use_container_width=True)

    with st.expander("👥 User Segments Details"):
        st.dataframe(
            engagement_metrics["user_segments"].head(100), use_container_width=True
        )

    with st.expander("📋 Retention Data"):
        st.dataframe(cohort_counts.head(50), use_container_width=True)
