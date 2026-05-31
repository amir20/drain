"""Analysis of descriptive telemetry attributes: versions, deployments, browsers, etc.

These power the standalone descriptive charts and the sidebar segmentation. All
functions operate on the events dataframe produced by ``data_loader`` (which carries
``UserID`` plus the descriptive columns) unless stated otherwise.
"""

from datetime import timedelta

import polars as pl
from config import BASELINE

# Ordered deployment-size buckets (by RunningContainers) for consistent chart ordering.
CONTAINER_BUCKETS = ["0", "1-5", "6-20", "21-50", "51-200", "200+"]


def _week_index(col: pl.Expr) -> pl.Expr:
    """Whole weeks between ``col`` and the cohort baseline (matches cohort_analysis)."""
    return ((col - pl.lit(BASELINE)) / timedelta(weeks=1)).cast(pl.Int64)


def _week_date(col: pl.Expr) -> pl.Expr:
    """Convert a week index back to the date of that week's start."""
    return pl.lit(BASELINE) + col.cast(pl.Int64) * pl.duration(weeks=1)


# Feature-flag columns surfaced in adoption charts and segmentation.
FEATURE_FLAGS = [
    "HasActions",
    "HasHostname",
    "HasCustomAddress",
    "HasCustomBase",
    "HasShell",
]


def container_bucket_expr(col: pl.Expr) -> pl.Expr:
    """Bucket a RunningContainers count into an ordered deployment-size band."""
    return (
        pl.when(col.is_null())
        .then(pl.lit("Unknown"))
        .when(col == 0)
        .then(pl.lit("0"))
        .when(col <= 5)
        .then(pl.lit("1-5"))
        .when(col <= 20)
        .then(pl.lit("6-20"))
        .when(col <= 50)
        .then(pl.lit("21-50"))
        .when(col <= 200)
        .then(pl.lit("51-200"))
        .otherwise(pl.lit("200+"))
    )


def browser_family_expr(col: pl.Expr) -> pl.Expr:
    """Map a raw user-agent string to a browser family.

    Order matters: Edge/Opera advertise "Chrome", and Chrome advertises "Safari", so
    the more specific tokens are checked first.
    """
    return (
        pl.when(col.is_null())
        .then(pl.lit("Unknown"))
        .when(col.str.contains("Edg/") | col.str.contains("Edge/"))
        .then(pl.lit("Edge"))
        .when(col.str.contains("OPR/") | col.str.contains("Opera"))
        .then(pl.lit("Opera"))
        .when(col.str.contains("Firefox/"))
        .then(pl.lit("Firefox"))
        .when(col.str.contains("Chrome/"))
        .then(pl.lit("Chrome"))
        .when(col.str.contains("Safari/"))
        .then(pl.lit("Safari"))
        .otherwise(pl.lit("Other"))
    )


def os_family_expr(col: pl.Expr) -> pl.Expr:
    """Map a raw user-agent string to an OS family.

    Order matters: Android and iPad user-agents also contain "Linux"/"Mac OS X", so
    the mobile platforms are checked before the desktop ones.
    """
    return (
        pl.when(col.is_null())
        .then(pl.lit("Unknown"))
        .when(col.str.contains("Windows"))
        .then(pl.lit("Windows"))
        .when(col.str.contains("Android"))
        .then(pl.lit("Android"))
        .when(col.str.contains("iPhone") | col.str.contains("iPad") | col.str.contains("iPod"))
        .then(pl.lit("iOS"))
        .when(col.str.contains("Mac OS X") | col.str.contains("Macintosh"))
        .then(pl.lit("macOS"))
        .when(col.str.contains("Linux") | col.str.contains("X11"))
        .then(pl.lit("Linux"))
        .otherwise(pl.lit("Other"))
    )


def calculate_install_attributes(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse the events to one row per install with its latest reported attributes.

    Used both for the deployment-size / browser distributions and as the lookup table
    that backs sidebar segmentation. Attributes are taken from each install's most
    recent event so segmentation reflects its current state.

    Args:
        df: Events dataframe with ``UserID``, ``CreatedAt``, and descriptive columns.

    Returns:
        pl.DataFrame: One row per ``UserID`` with derived ``container_bucket``,
        ``browser_family``, and ``os_family`` columns.
    """
    latest = df.sort("CreatedAt").group_by("UserID").last()
    return latest.with_columns(
        container_bucket_expr(pl.col("RunningContainers")).alias("container_bucket"),
        browser_family_expr(pl.col("Browser")).alias("browser_family"),
        os_family_expr(pl.col("Browser")).alias("os_family"),
    )


def filter_installs(install_attrs: pl.DataFrame, selections: dict) -> pl.Series:
    """Return the UserIDs whose install attributes match every active selection.

    Selections AND across attributes and OR within each attribute's value list. An
    empty (or missing) value list places no constraint on that attribute.

    Args:
        install_attrs: Output of :func:`calculate_install_attributes`.
        selections: Mapping of attribute column -> list of allowed values.

    Returns:
        pl.Series: Matching ``UserID`` values.
    """
    matched = install_attrs
    for column, allowed in selections.items():
        if allowed:
            matched = matched.filter(pl.col(column).is_in(allowed))
    return matched["UserID"]


def calculate_version_adoption(df: pl.DataFrame, top_n: int = 8) -> pl.DataFrame:
    """Weekly share of active installs running each version (top N, rest as 'Other').

    Args:
        df: Events with ``UserID``, ``current_week``, and ``Version``.
        top_n: Number of most-common versions to keep distinct.

    Returns:
        pl.DataFrame: ``current_week``, ``version``, ``installs``, ``share``, ``week_date``.
    """
    weekly = df.group_by(["current_week", "Version"]).agg(
        pl.col("UserID").n_unique().alias("installs")
    )
    top = (
        weekly.group_by("Version")
        .agg(pl.col("installs").sum().alias("total"))
        .sort("total", descending=True)
        .head(top_n)["Version"]
        .to_list()
    )
    weekly = (
        weekly.with_columns(
            pl.when(pl.col("Version").is_in(top))
            .then(pl.col("Version"))
            .otherwise(pl.lit("Other"))
            .alias("version")
        )
        .group_by(["current_week", "version"])
        .agg(pl.col("installs").sum().alias("installs"))
    )
    totals = df.group_by("current_week").agg(pl.col("UserID").n_unique().alias("active"))
    return (
        weekly.join(totals, on="current_week")
        .with_columns(
            (pl.col("installs") / pl.col("active")).alias("share"),
            _week_date(pl.col("current_week")).alias("week_date"),
        )
        .sort(["current_week", "version"])
    )


def calculate_new_installs(start_df: pl.DataFrame) -> pl.DataFrame:
    """Weekly new installs and launch volume from the start (app-launch) beacons.

    Args:
        start_df: Start beacons with ``UserID`` and ``CreatedAt``.

    Returns:
        pl.DataFrame: ``week``, ``new_installs`` (installs launching for the first time
        that week), ``active_installs``, ``launches`` (total beacons), ``week_date``.
    """
    starts = start_df.with_columns(_week_index(pl.col("CreatedAt")).alias("week"))
    first_week = (
        starts.group_by("UserID")
        .agg(pl.col("week").min().alias("first_week"))
        .group_by("first_week")
        .agg(pl.len().alias("new_installs"))
    )
    weekly = starts.group_by("week").agg(
        pl.col("UserID").n_unique().alias("active_installs"),
        pl.len().alias("launches"),
    )
    return (
        weekly.join(first_week, left_on="week", right_on="first_week", how="left")
        .with_columns(pl.col("new_installs").fill_null(0))
        .with_columns(_week_date(pl.col("week")).alias("week_date"))
        .sort("week")
    )


def calculate_deployment_scale(install_attrs: pl.DataFrame) -> pl.DataFrame:
    """Distribution of installs across deployment-size buckets.

    Args:
        install_attrs: Output of :func:`calculate_install_attributes`.

    Returns:
        pl.DataFrame: ``container_bucket``, ``installs``, ``share`` (bucket-ordered).
    """
    order = {bucket: i for i, bucket in enumerate([*CONTAINER_BUCKETS, "Unknown"])}
    total = install_attrs.height
    dist = install_attrs.group_by("container_bucket").agg(pl.len().alias("installs"))
    return (
        dist.with_columns(
            (pl.col("installs") / total).alias("share"),
            pl.col("container_bucket").replace_strict(order, default=len(order)).alias("_order"),
        )
        .sort("_order")
        .drop("_order")
    )


def calculate_auth_mix(df: pl.DataFrame) -> pl.DataFrame:
    """Weekly share of active installs by auth provider.

    Args:
        df: Events with ``UserID``, ``current_week``, and ``AuthProvider``.

    Returns:
        pl.DataFrame: ``current_week``, ``AuthProvider``, ``installs``, ``share``, ``week_date``.
    """
    weekly = df.group_by(["current_week", "AuthProvider"]).agg(
        pl.col("UserID").n_unique().alias("installs")
    )
    totals = df.group_by("current_week").agg(pl.col("UserID").n_unique().alias("active"))
    return (
        weekly.join(totals, on="current_week")
        .with_columns(
            (pl.col("installs") / pl.col("active")).alias("share"),
            _week_date(pl.col("current_week")).alias("week_date"),
        )
        .sort(["current_week", "AuthProvider"])
    )


def calculate_concurrent_clients(df: pl.DataFrame) -> pl.DataFrame:
    """Weekly average and maximum concurrent browser clients per install.

    Each install's weekly peak Clients is taken first, then averaged across installs so
    busy installs don't dominate via beacon count.

    Args:
        df: Events with ``UserID``, ``current_week``, and ``Clients``.

    Returns:
        pl.DataFrame: ``current_week``, ``avg_clients``, ``max_clients``, ``week_date``.
    """
    peaks = df.group_by(["UserID", "current_week"]).agg(pl.col("Clients").max().alias("peak"))
    return (
        peaks.group_by("current_week")
        .agg(
            pl.col("peak").mean().alias("avg_clients"),
            pl.col("peak").max().alias("max_clients"),
        )
        .with_columns(_week_date(pl.col("current_week")).alias("week_date"))
        .sort("current_week")
    )


def calculate_feature_adoption(df: pl.DataFrame) -> pl.DataFrame:
    """Weekly share of active installs that have each feature flag enabled.

    A null flag (older data that predates the feature) counts as not adopted.

    Args:
        df: Events with ``UserID``, ``current_week``, and the feature-flag columns.

    Returns:
        pl.DataFrame: ``current_week``, ``feature``, ``installs``, ``adoption``, ``week_date``.
    """
    per_install = df.group_by(["UserID", "current_week"]).agg(
        [pl.col(flag).fill_null(False).max().alias(flag) for flag in FEATURE_FLAGS]
    )
    totals = per_install.group_by("current_week").agg(pl.len().alias("installs"))
    long = per_install.unpivot(
        index=["UserID", "current_week"],
        on=FEATURE_FLAGS,
        variable_name="feature",
        value_name="enabled",
    )
    return (
        long.group_by(["current_week", "feature"])
        .agg(pl.col("enabled").sum().alias("enabled_installs"))
        .join(totals, on="current_week")
        .with_columns(
            (pl.col("enabled_installs") / pl.col("installs")).alias("adoption"),
            _week_date(pl.col("current_week")).alias("week_date"),
        )
        .sort(["current_week", "feature"])
    )


def _share_distribution(install_attrs: pl.DataFrame, column: str) -> pl.DataFrame:
    total = install_attrs.height
    return (
        install_attrs.group_by(column)
        .agg(pl.len().alias("installs"))
        .with_columns((pl.col("installs") / total).alias("share"))
        .sort("installs", descending=True)
    )


def calculate_browser_mix(install_attrs: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Distribution of installs by browser family and by OS family.

    Args:
        install_attrs: Output of :func:`calculate_install_attributes`.

    Returns:
        tuple: ``(browser_df, os_df)``, each with ``installs`` and ``share``.
    """
    return (
        _share_distribution(install_attrs, "browser_family"),
        _share_distribution(install_attrs, "os_family"),
    )
