import polars as pl
import streamlit as st
import glob
from datetime import datetime, timedelta, timezone
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from typing import Dict


# Configuration
st.set_page_config(
    page_title="Dozzle Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "Dozzle Analytics Dashboard - Comprehensive user behavior analysis"
    },
)

# Constants
BASELINE = datetime(year=2020, month=1, day=1).replace(tzinfo=timezone.utc)
DATA_PATH = "./data/day-*.parquet"

# Custom CSS for modern styling
st.markdown(
    """
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 2rem;
        text-align: center;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }
    .insight-box {
        background-color: #f8fafc;
        border-left: 4px solid #3b82f6;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_and_process_data() -> pl.DataFrame:
    """Load parquet files and perform initial data processing."""
    parquet_files = glob.glob(DATA_PATH)
    if not parquet_files:
        st.error("No data files found. Please check the data path.")
        return pl.DataFrame()

    df = pl.concat([pl.read_parquet(file) for file in parquet_files], how="diagonal")

    # Filter and clean data
    df = (
        df.filter(df["Name"] == "events")
        .with_columns(
            pl.when(pl.col("ServerID").is_null() | (pl.col("ServerID") == ""))
            .then(pl.col("RemoteIP").hash())
            .otherwise(pl.col("ServerID").hash())
            .alias("UserID")
        )
        .drop(
            [
                "RemoteIP",
                "HasHostname",
                "FilterLength",
                "Clients",
                "HasActions",
                "Browser",
                "ServerID",
                "HasCustomAddress",
                "HasCustomBase",
                "IsSwarmMode",
                "RemoteClients",
                "RemoteAgents",
            ]
        )
    )

    return df


def compute_cohort_data(df: pl.DataFrame) -> pl.DataFrame:
    """Compute cohort analysis data."""
    # Add activation date for each user
    df = df.with_columns(pl.col("CreatedAt").min().over("UserID").alias("activated"))

    # Calculate week numbers from baseline
    df = df.with_columns(
        [
            ((pl.col("activated") - pl.lit(BASELINE)) / timedelta(weeks=1))
            .cast(pl.Int64)
            .alias("activated_week"),
            ((pl.col("CreatedAt") - pl.lit(BASELINE)) / timedelta(weeks=1))
            .cast(pl.Int64)
            .alias("current_week"),
        ]
    )

    # Add cohort index and day of week
    df = df.with_columns(
        [
            (pl.col("current_week") - pl.col("activated_week")).alias("cohort_index"),
            pl.col("CreatedAt").dt.weekday().alias("day_of_week"),
            pl.col("CreatedAt").dt.hour().alias("hour_of_day"),
        ]
    )

    return df


def calculate_cohort_retention(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate cohort retention rates."""
    cohort_counts = (
        df.group_by(["activated_week", "cohort_index"])
        .agg(pl.col("UserID").n_unique().alias("users"))
        .sort("activated_week")
    )

    # Calculate retention rate and add activation date
    cohort_counts = cohort_counts.with_columns(
        [
            (pl.col("users") / pl.col("users").max().over("activated_week")).alias(
                "retention_rate"
            ),
            (
                pl.lit(BASELINE)
                + pl.col("activated_week").cast(pl.Int64) * pl.duration(weeks=1)
            ).alias("activated_date"),
        ]
    )

    return cohort_counts


def calculate_growth_metrics(df: pl.DataFrame) -> Dict:
    """Calculate user growth and engagement metrics."""
    # New vs returning users by week
    user_first_seen = df.group_by("UserID").agg(
        pl.col("CreatedAt").min().alias("first_seen")
    )

    weekly_data = df.join(user_first_seen, on="UserID").with_columns(
        [
            ((pl.col("CreatedAt") - pl.lit(BASELINE)) / timedelta(weeks=1))
            .cast(pl.Int64)
            .alias("week"),
            ((pl.col("first_seen") - pl.lit(BASELINE)) / timedelta(weeks=1))
            .cast(pl.Int64)
            .alias("first_week"),
        ]
    )

    # Calculate new users by week
    new_users_by_week = (
        weekly_data.filter(pl.col("week") == pl.col("first_week"))
        .group_by("week")
        .agg(pl.col("UserID").n_unique().alias("new_users"))
    )

    # Calculate active users and total events by week
    weekly_stats = (
        weekly_data.group_by("week")
        .agg(
            [
                pl.col("UserID").n_unique().alias("active_users"),
                pl.len().alias("total_events"),
            ]
        )
        .join(new_users_by_week, on="week", how="left")
        .with_columns(
            [
                pl.col("new_users").fill_null(0),
                (pl.col("active_users") - pl.col("new_users").fill_null(0)).alias(
                    "returning_users"
                ),
                (pl.lit(BASELINE) + pl.col("week") * pl.duration(weeks=1)).alias(
                    "week_date"
                ),
            ]
        )
        .sort("week")
    )

    # Calculate growth rates
    weekly_stats = weekly_stats.with_columns(
        [
            (pl.col("new_users").pct_change() * 100).alias("new_user_growth_rate"),
            (pl.col("active_users").pct_change() * 100).alias(
                "active_user_growth_rate"
            ),
        ]
    )

    return {
        "weekly_stats": weekly_stats,
        "total_users": df["UserID"].n_unique(),
        "total_events": len(df),
        "avg_events_per_user": len(df) / df["UserID"].n_unique(),
        "date_range": (df["CreatedAt"].min(), df["CreatedAt"].max()),
    }


def calculate_engagement_metrics(df: pl.DataFrame) -> Dict:
    """Calculate user engagement patterns."""
    # Daily activity patterns
    daily_activity = (
        df.group_by("hour_of_day")
        .agg(
            [
                pl.col("UserID").n_unique().alias("unique_users"),
                pl.len().alias("events"),
            ]
        )
        .sort("hour_of_day")
    )

    # Weekly activity patterns
    weekly_activity = (
        df.group_by("day_of_week")
        .agg(
            [
                pl.col("UserID").n_unique().alias("unique_users"),
                pl.len().alias("events"),
            ]
        )
        .sort("day_of_week")
    )

    # User segments based on activity
    user_activity = df.group_by("UserID").agg(
        [
            pl.len().alias("total_events"),
            pl.col("CreatedAt").count().alias("session_count"),
            (pl.col("CreatedAt").max() - pl.col("CreatedAt").min())
            .dt.total_days()
            .alias("lifetime_days"),
        ]
    )

    # Add user segments in a separate operation to avoid column reference issues
    user_activity = user_activity.with_columns(
        pl.when(pl.col("total_events") >= 100)
        .then(pl.lit("Power User"))
        .when(pl.col("total_events") >= 50)
        .then(pl.lit("Regular User"))
        .when(pl.col("total_events") >= 10)
        .then(pl.lit("Casual User"))
        .otherwise(pl.lit("New User"))
        .alias("user_segment")
    )

    segment_distribution = user_activity.group_by("user_segment").agg(
        pl.len().alias("user_count")
    )

    return {
        "daily_activity": daily_activity,
        "weekly_activity": weekly_activity,
        "user_segments": user_activity,
        "segment_distribution": segment_distribution,
    }


def display_key_metrics(growth_metrics: Dict):
    """Display key performance indicators."""
    st.markdown(
        '<div class="main-header">📊 Dozzle Analytics Dashboard</div>',
        unsafe_allow_html=True,
    )

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
            color_continuous_scale="viridis",
        )
        fig_hourly.update_layout(
            xaxis_title="Hour of Day", yaxis_title="Unique Users", showlegend=False
        )
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col2:
        # Day of week activity
        weekly_activity = engagement_metrics["weekly_activity"]
        # Polars weekday: 1=Monday, 2=Tuesday, ..., 7=Sunday
        days_map = {
            1: "Monday",
            2: "Tuesday",
            3: "Wednesday",
            4: "Thursday",
            5: "Friday",
            6: "Saturday",
            7: "Sunday",
        }

        weekly_df = weekly_activity.to_pandas()
        weekly_df["day_name"] = [
            days_map.get(i, f"Day {i}") for i in weekly_df["day_of_week"]
        ]

        fig_weekly = px.bar(
            weekly_df,
            x="day_name",
            y="unique_users",
            title="User Activity by Day of Week",
            color="unique_users",
            color_continuous_scale="plasma",
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


def display_retention_heatmap(retention: pl.DataFrame):
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
        color_continuous_scale="RdYlGn",
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


def prepare_retention_matrix(cohort_counts: pl.DataFrame) -> pl.DataFrame:
    """Prepare retention data for heatmap visualization."""
    retention = (
        cohort_counts.pivot(
            on="cohort_index",
            index="activated_date",
            values="retention_rate",
            aggregate_function="first",
            sort_columns=True,
        )
        .tail(15)  # Show more cohorts
        .select(["activated_date"] + [str(i) for i in range(12)])  # Show more weeks
    )

    return retention


def display_advanced_analytics(df: pl.DataFrame):
    """Display advanced analytics section."""
    st.header("🔬 Advanced Analytics")

    # Churn analysis
    st.subheader("⚠️ Churn Risk Analysis")

    # Calculate days since last activity
    current_date = df["CreatedAt"].max()
    user_last_activity = (
        df.group_by("UserID")
        .agg(
            [
                pl.col("CreatedAt").max().alias("last_activity"),
                pl.len().alias("total_events"),
            ]
        )
        .with_columns(
            (current_date - pl.col("last_activity"))
            .dt.total_days()
            .alias("days_inactive")
        )
    )

    # Add user value and churn status in separate operations
    user_last_activity = user_last_activity.with_columns(
        pl.when(pl.col("total_events") >= 100)
        .then(pl.lit("High Value"))
        .when(pl.col("total_events") >= 20)
        .then(pl.lit("Medium Value"))
        .otherwise(pl.lit("Low Value"))
        .alias("user_value")
    ).with_columns(
        pl.when(pl.col("days_inactive") <= 7)
        .then(pl.lit("Active"))
        .when(pl.col("days_inactive") <= 30)
        .then(pl.lit("At Risk"))
        .when(pl.col("days_inactive") <= 90)
        .then(pl.lit("Churning"))
        .otherwise(pl.lit("Churned"))
        .alias("churn_status")
    )

    churn_summary = user_last_activity.group_by(["churn_status", "user_value"]).agg(
        pl.len().alias("user_count")
    )

    # Display churn matrix
    churn_pivot = churn_summary.pivot(
        on="user_value",
        index="churn_status",
        values="user_count",
        aggregate_function="sum",
    ).fill_null(0)

    fig_churn = px.imshow(
        churn_pivot.to_pandas().set_index("churn_status"),
        title="User Churn Risk by Value Segment",
        color_continuous_scale="Reds",
        text_auto=True,
    )

    st.plotly_chart(fig_churn, use_container_width=True)


def main():
    """Main dashboard function."""
    # Sidebar for filters and controls
    st.sidebar.title("🎛️ Dashboard Controls")

    # Date range filter
    st.sidebar.subheader("📅 Date Range")

    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto-refresh data", value=False)
    if auto_refresh:
        st.sidebar.write("🔄 Data refreshes every 5 minutes")

    # Load and process data
    with st.spinner("🔄 Loading and processing data..."):
        df = load_and_process_data()

        if df.is_empty():
            st.error("No data available to display.")
            return

        df = compute_cohort_data(df)

    # Calculate all metrics
    with st.spinner("📊 Calculating metrics..."):
        growth_metrics = calculate_growth_metrics(df)
        engagement_metrics = calculate_engagement_metrics(df)
        cohort_counts = calculate_cohort_retention(df)
        retention = prepare_retention_matrix(cohort_counts)

    # Display dashboard sections
    display_key_metrics(growth_metrics)

    # Create tabs for better organization
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 Growth", "🎯 Engagement", "🔄 Retention", "🔬 Advanced"]
    )

    with tab1:
        display_growth_analysis(growth_metrics)

        with st.expander("📋 Raw Growth Data"):
            st.dataframe(
                growth_metrics["weekly_stats"].tail(20), use_container_width=True
            )

    with tab2:
        display_engagement_analysis(engagement_metrics)

        with st.expander("👥 User Segments Details"):
            st.dataframe(
                engagement_metrics["user_segments"].head(100), use_container_width=True
            )

    with tab3:
        display_retention_heatmap(retention)

        with st.expander("📋 Retention Data"):
            st.dataframe(cohort_counts.head(50), use_container_width=True)

    with tab4:
        display_advanced_analytics(df)

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #6b7280;'>Built with ❤️ using Streamlit and Plotly</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
