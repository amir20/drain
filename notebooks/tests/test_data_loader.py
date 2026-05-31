"""Tests for data loading, identity construction, and identity quality."""

from datetime import UTC, datetime

import polars as pl
from data_loader import (
    calculate_identity_quality,
    load_and_process_data,
    load_start_beacons,
)


def _ts(*days: int) -> pl.Series:
    return pl.Series(
        "CreatedAt",
        [datetime(2024, 1, d, tzinfo=UTC) for d in days],
        dtype=pl.Datetime("ns", "UTC"),
    )


def _write_day(path, *, names, created, server_ids, **cols):
    data = {
        "Name": names,
        "CreatedAt": created,
        "ServerID": pl.Series(server_ids, dtype=pl.String),
    }
    data.update(cols)
    pl.DataFrame(data).write_parquet(path)


def test_load_keeps_events_with_descriptive_columns_and_stable_identity(tmp_path):
    """Events load keeps charting columns, drops raw identity/dead columns, builds UserID.

    A 'start' row is excluded; rows without a ServerID fall back to RemoteIP and are
    flagged id_from_ip (two sharing an IP get one UserID); a file with an extra column
    (HasShell drift) is tolerated; an always-dead column (RemoteClients) is dropped.
    """
    _write_day(
        tmp_path / "day-2024-01-01.parquet",
        names=["events", "events", "start"],
        created=_ts(1, 2, 3),
        server_ids=["srv-A", "", "srv-A"],
        RemoteIP=["10.0.0.1", "10.0.0.2", "10.0.0.1"],
        Version=["v1", "v1", "v1"],
        RunningContainers=[5, 9, 5],
        RemoteClients=[0, 0, 0],  # dead column -> must be dropped
    )
    _write_day(
        tmp_path / "day-2024-01-02.parquet",
        names=["events"],
        created=_ts(4),
        server_ids=pl.Series([None], dtype=pl.String),
        RemoteIP=["10.0.0.2"],
        Version=["v2"],
        RunningContainers=[2],
        HasShell=[True],  # newer-file column -> must be kept (null for old files)
    )

    df = load_and_process_data(str(tmp_path / "day-*.parquet"))

    assert df.height == 3
    assert (df["Name"] == "events").all()
    for kept in ("Version", "RunningContainers", "HasShell", "UserID", "id_from_ip"):
        assert kept in df.columns
    for dropped in ("RemoteIP", "ServerID", "RemoteClients"):
        assert dropped not in df.columns
    assert df["id_from_ip"].sum() == 2
    assert df.filter(pl.col("id_from_ip"))["UserID"].n_unique() == 1


def test_load_start_beacons_returns_launch_events_with_identity(tmp_path):
    """Start beacons are loaded separately with a UserID for new-install analysis."""
    _write_day(
        tmp_path / "day-2024-01-01.parquet",
        names=["start", "events", "start"],
        created=_ts(1, 2, 3),
        server_ids=["srv-A", "srv-B", "srv-A"],
        RemoteIP=["10.0.0.1", "10.0.0.2", "10.0.0.1"],
    )

    starts = load_start_beacons(str(tmp_path / "day-*.parquet"))

    assert starts.height == 2  # only the two 'start' rows
    assert {"UserID", "CreatedAt"}.issubset(starts.columns)
    assert starts["UserID"].n_unique() == 1  # both starts are srv-A


def test_identity_quality_reports_ip_derived_share():
    """Report what fraction of rows and distinct users were identified by IP fallback.

    Users 10 and 30 fall back to RemoteIP (no ServerID); user 20 has a ServerID.
    """
    df = pl.DataFrame(
        {
            "UserID": [10, 10, 20, 30, 20],
            "id_from_ip": [True, True, False, True, False],
        }
    )

    q = calculate_identity_quality(df)

    assert q["ip_rows"] == 3
    assert q["total_rows"] == 5
    assert abs(q["ip_row_pct"] - 0.6) < 1e-9
    assert q["ip_users"] == 2
    assert q["total_users"] == 3
    assert abs(q["ip_user_pct"] - 2 / 3) < 1e-9
