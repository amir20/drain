"""
Data processing module for the Dozzle Analytics Dashboard.

Contains all data loading, processing, and calculation functions.
"""

import polars as pl
import streamlit as st
import glob
from datetime import timedelta
from typing import Dict
from .config import (
    BASELINE,
    DATA_PATH,
    CACHE_TTL,
    USER_SEGMENTS,
    CHURN_THRESHOLDS,
    VALUE_SEGMENTS,
)


@st.cache_data(ttl=CACHE_TTL)
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
        pl.when(pl.col("total_events") >= USER_SEGMENTS["power_user"])
        .then(pl.lit("Power User"))
        .when(pl.col("total_events") >= USER_SEGMENTS["regular_user"])
        .then(pl.lit("Regular User"))
        .when(pl.col("total_events") >= USER_SEGMENTS["casual_user"])
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


def calculate_churn_metrics(df: pl.DataFrame) -> Dict:
    """Calculate churn risk analysis data."""
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
        pl.when(pl.col("total_events") >= VALUE_SEGMENTS["high_value"])
        .then(pl.lit("High Value"))
        .when(pl.col("total_events") >= VALUE_SEGMENTS["medium_value"])
        .then(pl.lit("Medium Value"))
        .otherwise(pl.lit("Low Value"))
        .alias("user_value")
    ).with_columns(
        pl.when(pl.col("days_inactive") <= CHURN_THRESHOLDS["active"])
        .then(pl.lit("Active"))
        .when(pl.col("days_inactive") <= CHURN_THRESHOLDS["at_risk"])
        .then(pl.lit("At Risk"))
        .when(pl.col("days_inactive") <= CHURN_THRESHOLDS["churning"])
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

    return {
        "user_last_activity": user_last_activity,
        "churn_summary": churn_summary,
        "churn_pivot": churn_pivot,
    }


def prepare_retention_matrix(cohort_counts: pl.DataFrame) -> pl.DataFrame:
    """Prepare retention data for heatmap visualization."""
    from .config import RETENTION_MATRIX_CONFIG

    retention = (
        cohort_counts.pivot(
            on="cohort_index",
            index="activated_date",
            values="retention_rate",
            aggregate_function="first",
            sort_columns=True,
        )
        .tail(RETENTION_MATRIX_CONFIG["cohort_count"])
        .select(
            ["activated_date"]
            + [str(i) for i in range(RETENTION_MATRIX_CONFIG["week_count"])]
        )
    )

    return retention
