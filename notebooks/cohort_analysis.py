"""Cohort analysis calculations for retention metrics."""

from datetime import timedelta
import polars as pl

from config import BASELINE, RETENTION_MATRIX_TAIL, RETENTION_MATRIX_WEEKS


def compute_cohort_data(df: pl.DataFrame) -> pl.DataFrame:
    """Compute cohort analysis data.

    Args:
        df: Input dataframe with UserID and CreatedAt columns.

    Returns:
        pl.DataFrame: Dataframe with cohort metrics added.
    """
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
    """Calculate cohort retention rates.

    Args:
        df: Dataframe with cohort data computed.

    Returns:
        pl.DataFrame: Cohort counts with retention rates.
    """
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
    """Prepare retention data for heatmap visualization.

    Args:
        cohort_counts: Cohort counts with retention rates.

    Returns:
        pl.DataFrame: Pivoted retention matrix.
    """
    retention = (
        cohort_counts.pivot(
            on="cohort_index",
            index="activated_date",
            values="retention_rate",
            aggregate_function="first",
            sort_columns=True,
        )
        .tail(RETENTION_MATRIX_TAIL)
        .select(["activated_date"] + [str(i) for i in range(RETENTION_MATRIX_WEEKS)])
    )

    return retention
