"""Engagement analysis calculations for user behavior metrics."""

from datetime import timedelta
from typing import Tuple
import polars as pl

from config import BASELINE


def calculate_user_lifecycle_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate user lifecycle metrics including new, retained, churned, and resurrected users.

    Args:
        df: Dataframe with UserID and current_week columns.

    Returns:
        pl.DataFrame: Weekly lifecycle metrics.
    """
    # Get unique users per week
    weekly_users = (
        df.group_by("current_week")
        .agg(pl.col("UserID").unique().alias("users"))
        .sort("current_week")
    )

    # Calculate lifecycle metrics
    lifecycle_metrics = []
    for i, row in enumerate(weekly_users.iter_rows(named=True)):
        week = row["current_week"]
        current_users = set(row["users"])

        if i == 0:
            # First week - all users are new
            new_users = len(current_users)
            retained_users = 0
            churned_users = 0
            resurrected_users = 0
            prev_users = current_users
        else:
            # Users from previous week
            retained_users = len(current_users & prev_users)
            new_users = len(current_users - prev_users - all_previous_users)
            resurrected_users = len(current_users & all_previous_users - prev_users)
            churned_users = len(prev_users - current_users)

            all_previous_users = all_previous_users | prev_users
            prev_users = current_users

        if i == 0:
            all_previous_users = current_users
            prev_users = current_users

        lifecycle_metrics.append(
            {
                "current_week": week,
                "new_users": new_users,
                "retained_users": retained_users,
                "churned_users": churned_users,
                "resurrected_users": resurrected_users,
                "total_active_users": len(current_users),
            }
        )

    lifecycle_df = pl.DataFrame(lifecycle_metrics).with_columns(
        (
            pl.lit(BASELINE)
            + pl.col("current_week").cast(pl.Int64) * pl.duration(weeks=1)
        ).alias("week_date")
    )

    return lifecycle_df


def calculate_stickiness_metrics(df: pl.DataFrame) -> Tuple[pl.DataFrame, dict]:
    """Calculate stickiness metrics including DAU/MAU ratio equivalent (WAU/MAU).

    Args:
        df: Dataframe with UserID and current_week columns.

    Returns:
        Tuple containing:
            - stickiness_df: Weekly stickiness metrics
            - summary_stats: Dictionary with summary statistics
    """
    # Calculate weekly active users (WAU)
    wau = (
        df.group_by("current_week")
        .agg(pl.col("UserID").n_unique().alias("wau"))
        .sort("current_week")
    )

    # Calculate 4-week rolling active users (approximation of MAU)
    wau = wau.with_columns(
        pl.col("wau").rolling_max(window_size=4, min_periods=1).alias("mau_rolling_max")
    )

    # Calculate stickiness ratio (WAU / MAU)
    wau = wau.with_columns(
        (pl.col("wau") / pl.col("mau_rolling_max")).alias("stickiness_ratio")
    )

    # Add week date
    wau = wau.with_columns(
        (
            pl.lit(BASELINE)
            + pl.col("current_week").cast(pl.Int64) * pl.duration(weeks=1)
        ).alias("week_date")
    )

    # Calculate summary statistics
    summary_stats = {
        "avg_stickiness": wau.select(pl.col("stickiness_ratio").mean())[0, 0],
        "current_wau": wau.select(pl.col("wau").tail(1))[0, 0],
        "current_mau": wau.select(pl.col("mau_rolling_max").tail(1))[0, 0],
    }

    return wau, summary_stats


def calculate_engagement_depth(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate engagement depth metrics (distribution of user activity levels).

    Args:
        df: Dataframe with UserID and current_week columns.

    Returns:
        pl.DataFrame: Engagement depth distribution by week.
    """
    # Calculate events per user per week
    user_weekly_events = (
        df.group_by(["UserID", "current_week"])
        .agg(pl.len().alias("event_count"))
        .with_columns(
            pl.when(pl.col("event_count") == 1)
            .then(pl.lit("1_event"))
            .when(pl.col("event_count") <= 5)
            .then(pl.lit("2-5_events"))
            .when(pl.col("event_count") <= 20)
            .then(pl.lit("6-20_events"))
            .when(pl.col("event_count") <= 50)
            .then(pl.lit("21-50_events"))
            .otherwise(pl.lit("50+_events"))
            .alias("engagement_level")
        )
    )

    # Count users in each engagement level per week
    engagement_distribution = (
        user_weekly_events.group_by(["current_week", "engagement_level"])
        .agg(pl.col("UserID").n_unique().alias("user_count"))
        .sort(["current_week", "engagement_level"])
    )

    # Add week date
    engagement_distribution = engagement_distribution.with_columns(
        (
            pl.lit(BASELINE)
            + pl.col("current_week").cast(pl.Int64) * pl.duration(weeks=1)
        ).alias("week_date")
    )

    return engagement_distribution


def calculate_cohort_engagement_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate engagement metrics by cohort age.

    Args:
        df: Dataframe with cohort data already computed.

    Returns:
        pl.DataFrame: Average events per user by cohort age.
    """
    cohort_engagement = (
        df.group_by("cohort_index")
        .agg(
            [
                pl.len().alias("total_events"),
                pl.col("UserID").n_unique().alias("active_users"),
            ]
        )
        .with_columns(
            (pl.col("total_events") / pl.col("active_users")).alias(
                "avg_events_per_user"
            )
        )
        .sort("cohort_index")
    )

    return cohort_engagement
