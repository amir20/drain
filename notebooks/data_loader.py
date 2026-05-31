"""Data loading and processing utilities for retention analysis."""

from typing import cast

import polars as pl
from config import DATA_PATH

# Explicit read schema: exactly the columns we analyse/chart. Listing them here means
# the scan reads only these (projection pushdown), tolerates schema drift across files
# (HasShell only exists in newer files -> inserted as null for old ones), and silently
# ignores the always-empty "dead" telemetry columns (ServerVersion, SubCommand, Mode,
# RemoteAgents/Clients, FilterLength, IsSwarmMode) instead of carrying them around.
_SCAN_SCHEMA = {
    "Name": pl.String,
    "CreatedAt": pl.Datetime("ns", "UTC"),
    "ServerID": pl.String,
    "RemoteIP": pl.String,
    "Version": pl.String,
    "RunningContainers": pl.Int64,
    "AuthProvider": pl.String,
    "Clients": pl.Int64,
    "Browser": pl.String,
    "HasActions": pl.Boolean,
    "HasHostname": pl.Boolean,
    "HasCustomAddress": pl.Boolean,
    "HasCustomBase": pl.Boolean,
    "HasShell": pl.Boolean,
}

# Raw identity columns dropped once UserID/id_from_ip are derived from them.
_IDENTITY_COLUMNS = ["ServerID", "RemoteIP"]


def _scan(data_glob: str) -> pl.LazyFrame:
    """Lazily scan the daily parquet files and derive the stable identity columns."""
    return (
        pl.scan_parquet(
            data_glob,
            schema=_SCAN_SCHEMA,
            missing_columns="insert",
            extra_columns="ignore",
        )
        .with_columns(
            (pl.col("ServerID").is_null() | (pl.col("ServerID") == "")).alias("id_from_ip")
        )
        .with_columns(
            pl.when(pl.col("id_from_ip"))
            .then(pl.col("RemoteIP").hash())
            .otherwise(pl.col("ServerID").hash())
            .alias("UserID")
        )
    )


def load_and_process_data(data_glob: str = DATA_PATH) -> pl.DataFrame:
    """Load the ``events`` beacons with the descriptive columns used for analysis.

    Uses a lazy scan so the ``Name == "events"`` predicate and the column projection are
    pushed into the parquet read instead of loading every row/column of every file into
    memory first. Keeps the descriptive columns (version, deployment size, auth, clients,
    browser, feature flags) for charting and segmentation, drops the raw identity columns
    once ``UserID``/``id_from_ip`` are derived, and flags IP-derived identities.

    Args:
        data_glob: Glob pattern matching the daily parquet files.

    Returns:
        pl.DataFrame: Events with UserID, id_from_ip, and descriptive columns.
    """
    lazy = _scan(data_glob).filter(pl.col("Name") == "events").drop(_IDENTITY_COLUMNS, strict=False)
    return cast(pl.DataFrame, lazy.collect())


def load_start_beacons(data_glob: str = DATA_PATH) -> pl.DataFrame:
    """Load the ``start`` (app-launch) beacons for new-install analysis.

    These are a distinct ~45% of rows that the events pipeline filters out; they are a
    cleaner launch/relaunch signal than inferring activation from periodic events.

    Args:
        data_glob: Glob pattern matching the daily parquet files.

    Returns:
        pl.DataFrame: Launch beacons with ``UserID`` and ``CreatedAt``.
    """
    lazy = _scan(data_glob).filter(pl.col("Name") == "start").select("UserID", "CreatedAt")
    return cast(pl.DataFrame, lazy.collect())


def calculate_identity_quality(df: pl.DataFrame) -> dict:
    """Report how much of the data relies on the RemoteIP identity fallback.

    UserIDs derived from RemoteIP (no ServerID) are unstable across IP changes and
    collapse users behind shared IPs, distorting every cohort/retention metric. This
    surfaces the magnitude so the dashboard can caveat the numbers.

    Args:
        df: Dataframe with ``UserID`` and ``id_from_ip`` columns.

    Returns:
        dict: Row/user counts and the IP-derived fraction of each.
    """
    total_rows = df.height
    ip_rows = int(df.select(pl.col("id_from_ip").sum()).item())
    total_users = int(df.select(pl.col("UserID").n_unique()).item())
    ip_users = int(df.filter(pl.col("id_from_ip")).select(pl.col("UserID").n_unique()).item())

    return {
        "total_rows": total_rows,
        "ip_rows": ip_rows,
        "ip_row_pct": ip_rows / total_rows if total_rows else 0.0,
        "total_users": total_users,
        "ip_users": ip_users,
        "ip_user_pct": ip_users / total_users if total_users else 0.0,
    }
