"""Tests for engagement analysis metrics."""

import polars as pl
from engagement_analysis import calculate_stickiness_metrics


def test_mau_counts_distinct_users_over_trailing_four_weeks():
    """MAU must be distinct users across the trailing 4 weeks, not max weekly WAU.

    A, B active wk0; C wk1; D wk2; A again wk3. MAU(wk3) spans weeks 0-3 and must
    count {A, B, C, D} = 4. The old rolling_max(WAU) approximation would report 2.
    """
    df = pl.DataFrame(
        {
            "UserID": [1, 2, 3, 4, 1],
            "current_week": [0, 0, 1, 2, 3],
        }
    )

    stickiness, stats = calculate_stickiness_metrics(df)

    by_week = {row["current_week"]: row["mau"] for row in stickiness.iter_rows(named=True)}
    assert by_week == {0: 2, 1: 3, 2: 4, 3: 4}
    assert stats["current_mau"] == 4

    wk3 = stickiness.filter(pl.col("current_week") == 3)
    assert wk3["wau"][0] == 1
    assert abs(wk3["stickiness_ratio"][0] - 0.25) < 1e-9
