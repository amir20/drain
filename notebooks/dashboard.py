import polars as pl
import streamlit as st
import glob
from datetime import datetime, timedelta, timezone
import plotly.express as px
import plotly.graph_objects as go
from typing import Tuple


# Configuration
st.set_page_config(page_title="Dozzle Retention Analysis", layout="wide")

# Constants
BASELINE = datetime(year=2020, month=1, day=1).replace(tzinfo=timezone.utc)
DATA_PATH = "./data/day-*.parquet"


def load_and_process_data() -> pl.DataFrame:
    """Load parquet files and perform initial data processing."""
    parquet_files = glob.glob(DATA_PATH)
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

    # Add cohort index
    df = df.with_columns(
        (pl.col("current_week") - pl.col("activated_week")).alias("cohort_index")
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
        .tail(10)
        .select(["activated_date"] + [str(i) for i in range(8)])
    )

    return retention


def display_retention_heatmap(retention: pl.DataFrame) -> None:
    """Display cohort retention heatmap."""
    retention_pd = retention.to_pandas()

    if "activated_date" in retention_pd.columns:
        retention_pd = retention_pd.set_index("activated_date")

    fig = px.imshow(
        retention_pd,
        labels=dict(x="Week", y="Cohort", color="Retention Rate"),
        x=retention_pd.columns,
        y=retention_pd.index,
        color_continuous_scale="Viridis",
        zmin=0,
        zmax=1,
        aspect="auto",
        text_auto=".0%",
    )

    fig_go = go.Figure(fig)
    fig_go.data[0].xgap = 2
    fig_go.data[0].ygap = 2

    fig_go.update_layout(
        title="Cohort Retention Analysis",
        xaxis_title="Week Number",
        yaxis_title="Cohort Start Date",
        coloraxis_colorbar=dict(title="Retention Rate"),
        height=600,
        width=900,
    )

    st.plotly_chart(fig_go, use_container_width=True)


def calculate_usage_frequency(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Calculate usage frequency metrics."""
    usage_frequency = (
        df.group_by(["UserID", "current_week"])
        .agg(pl.len().alias("events_count"))
        .group_by("current_week")
        .agg(
            pl.col("events_count").mean().alias("avg_events_per_user_per_week"),
            pl.col("UserID").n_unique().alias("active_users"),
        )
    )

    usage_frequency = usage_frequency.with_columns(
        (
            pl.lit(BASELINE)
            + pl.col("current_week").cast(pl.Int64) * pl.duration(weeks=1)
        ).alias("week_date")
    ).sort("current_week")

    overall_avg = (
        df.group_by(["UserID", "current_week"])
        .agg(pl.len().alias("events_count"))
        .select(
            pl.col("events_count").mean().alias("overall_avg_events_per_user_per_week")
        )
    )

    return usage_frequency, overall_avg


def display_usage_frequency_analysis(
    usage_frequency: pl.DataFrame, overall_avg: pl.DataFrame
) -> None:
    """Display usage frequency analysis section."""
    st.header("Usage Frequency Analysis")

    # Metrics
    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Overall Average Events per User per Week",
            f"{overall_avg['overall_avg_events_per_user_per_week'][0]:.2f}",
        )

    with col2:
        recent_avg = usage_frequency.tail(4).select(
            pl.col("avg_events_per_user_per_week").mean()
        )[0, 0]
        st.metric("Recent Average (Last 4 Weeks)", f"{recent_avg:.2f}")

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
            ).tail(20)
        )


def main() -> None:
    """Main dashboard function."""
    st.title("Cohort Retention Analysis")

    # Load and process data
    with st.spinner("Loading and processing data..."):
        df = load_and_process_data()
        df = compute_cohort_data(df)

    # Calculate cohort retention
    with st.spinner("Calculating cohort retention rates..."):
        cohort_counts = calculate_cohort_retention(df)
        retention = prepare_retention_matrix(cohort_counts)

    # Display cohort retention heatmap
    with st.spinner("Generating retention heatmap..."):
        display_retention_heatmap(retention)

    # Show data details
    with st.expander("Show top 50 rows"):
        st.dataframe(cohort_counts.head(50))

    # Calculate and display usage frequency analysis
    with st.spinner("Calculating usage frequency metrics..."):
        usage_frequency, overall_avg = calculate_usage_frequency(df)
        display_usage_frequency_analysis(usage_frequency, overall_avg)


if __name__ == "__main__":
    main()
