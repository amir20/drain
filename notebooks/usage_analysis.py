"""Usage frequency analysis calculations."""

from datetime import timedelta
from typing import Tuple
import polars as pl

from config import BASELINE


def calculate_usage_frequency(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Calculate usage frequency metrics.

    Args:
        df: Dataframe with UserID and current_week columns.

    Returns:
        Tuple containing:
            - usage_frequency: Weekly usage metrics
            - overall_avg: Overall average events per user per week
    """
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
