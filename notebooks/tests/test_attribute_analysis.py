"""Tests for descriptive-attribute analysis (versions, deployments, browsers, etc.)."""

from datetime import UTC, datetime

import polars as pl
from attribute_analysis import (
    browser_family_expr,
    calculate_auth_mix,
    calculate_browser_mix,
    calculate_concurrent_clients,
    calculate_deployment_scale,
    calculate_feature_adoption,
    calculate_install_attributes,
    calculate_new_installs,
    calculate_version_adoption,
    filter_installs,
    os_family_expr,
)


def _dt(day: int) -> datetime:
    return datetime(2025, 1, day, tzinfo=UTC)


CHROME_WIN = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
)
EDGE_WIN = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
)
FIREFOX_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0"
SAFARI_MAC = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
SAFARI_IOS = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)
CHROME_ANDROID = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)
CHROME_LINUX = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
)


def test_browser_and_os_families_resolve_with_correct_priority():
    """Edge/Android/iOS must win over the Chrome/Linux/macOS substrings they contain."""
    df = pl.DataFrame(
        {
            "Browser": [
                CHROME_WIN,
                EDGE_WIN,
                FIREFOX_WIN,
                SAFARI_MAC,
                SAFARI_IOS,
                CHROME_ANDROID,
                CHROME_LINUX,
                None,
            ]
        }
    )

    out = df.with_columns(
        browser_family_expr(pl.col("Browser")).alias("browser"),
        os_family_expr(pl.col("Browser")).alias("os"),
    )

    assert out["browser"].to_list() == [
        "Chrome",
        "Edge",
        "Firefox",
        "Safari",
        "Safari",
        "Chrome",
        "Chrome",
        "Unknown",
    ]
    assert out["os"].to_list() == [
        "Windows",
        "Windows",
        "Windows",
        "macOS",
        "iOS",
        "Android",
        "Linux",
        "Unknown",
    ]


def test_install_attributes_uses_latest_value_per_install():
    """Each install collapses to one row holding its most recent reported attributes."""
    df = pl.DataFrame(
        {
            "UserID": [1, 1, 2],
            "CreatedAt": [_dt(1), _dt(5), _dt(1)],
            "Version": ["v1", "v2", "v9"],
            "AuthProvider": ["none", "simple", "none"],
            "RunningContainers": [3, 30, 0],
            "Browser": [CHROME_WIN, FIREFOX_WIN, None],
        }
    )

    attrs = calculate_install_attributes(df)
    a1 = attrs.filter(pl.col("UserID") == 1).row(0, named=True)
    a2 = attrs.filter(pl.col("UserID") == 2).row(0, named=True)

    assert attrs.height == 2
    assert a1["Version"] == "v2"  # latest by CreatedAt
    assert a1["AuthProvider"] == "simple"
    assert a1["container_bucket"] == "21-50"
    assert a1["browser_family"] == "Firefox"
    assert a2["container_bucket"] == "0"
    assert a2["browser_family"] == "Unknown"


def test_filter_installs_matches_all_active_selections():
    """Selections AND across attributes, OR within a list; empty list = no constraint."""
    attrs = pl.DataFrame(
        {
            "UserID": [1, 2, 3, 4],
            "AuthProvider": ["none", "simple", "simple", "none"],
            "container_bucket": ["1-5", "21-50", "1-5", "200+"],
        }
    )

    ids = filter_installs(attrs, {"AuthProvider": ["simple"], "container_bucket": ["1-5"]})
    assert sorted(ids.to_list()) == [3]

    ids_or = filter_installs(attrs, {"AuthProvider": ["simple", "none"]})
    assert sorted(ids_or.to_list()) == [1, 2, 3, 4]

    ids_none = filter_installs(attrs, {"AuthProvider": [], "container_bucket": ["1-5"]})
    assert sorted(ids_none.to_list()) == [1, 3]


def test_version_adoption_keeps_top_n_and_buckets_rest_as_other():
    """Weekly install share per version; non-top versions collapse into 'Other'."""
    df = pl.DataFrame(
        {
            "UserID": [1, 2, 3, 4, 5],
            "current_week": [0, 0, 0, 0, 0],
            "Version": ["v1", "v1", "v2", "v2", "v3"],
        }
    )

    out = calculate_version_adoption(df, top_n=2)
    by_version = {r["version"]: r for r in out.iter_rows(named=True)}

    assert set(by_version) == {"v1", "v2", "Other"}
    assert by_version["v1"]["installs"] == 2
    assert abs(by_version["v1"]["share"] - 0.4) < 1e-9
    assert by_version["Other"]["installs"] == 1
    assert abs(by_version["Other"]["share"] - 0.2) < 1e-9
    assert "week_date" in out.columns


def test_new_installs_counts_first_ever_launch_week():
    """New installs = installs whose first-ever launch falls in that week; launches count all."""
    starts = pl.DataFrame(
        {
            "UserID": [1, 1, 2, 3, 3],
            "CreatedAt": [_dt(3), _dt(10), _dt(10), _dt(10), _dt(10)],
        }
    )

    out = calculate_new_installs(starts)
    weeks = sorted(out["week"].to_list())
    assert len(weeks) == 2
    early, late = weeks

    def row(w):
        return out.filter(pl.col("week") == w).row(0, named=True)

    assert row(early)["new_installs"] == 1  # user 1
    assert row(late)["new_installs"] == 2  # users 2 and 3 (user 1 already counted)
    assert row(late)["launches"] == 4  # user1 + user2 + user3 x2
    assert row(late)["active_installs"] == 3
    assert "week_date" in out.columns


def test_deployment_scale_distribution_is_ordered_with_shares():
    attrs = pl.DataFrame(
        {
            "UserID": [1, 2, 3, 4],
            "container_bucket": ["21-50", "1-5", "1-5", "0"],
        }
    )

    out = calculate_deployment_scale(attrs)
    assert out["container_bucket"].to_list() == ["0", "1-5", "21-50"]
    by_bucket = {r["container_bucket"]: r for r in out.iter_rows(named=True)}
    assert by_bucket["1-5"]["installs"] == 2
    assert abs(by_bucket["1-5"]["share"] - 0.5) < 1e-9


def test_auth_mix_weekly_share_by_provider():
    df = pl.DataFrame(
        {
            "UserID": [1, 2, 3],
            "current_week": [0, 0, 0],
            "AuthProvider": ["none", "none", "simple"],
        }
    )

    out = calculate_auth_mix(df)
    by_auth = {r["AuthProvider"]: r for r in out.iter_rows(named=True)}
    assert by_auth["none"]["installs"] == 2
    assert abs(by_auth["none"]["share"] - 2 / 3) < 1e-9
    assert abs(by_auth["simple"]["share"] - 1 / 3) < 1e-9
    assert "week_date" in out.columns


def test_concurrent_clients_averages_per_install_peaks():
    """Per install take the weekly peak Clients, then average those peaks across installs."""
    df = pl.DataFrame(
        {
            "UserID": [1, 1, 2],
            "current_week": [0, 0, 0],
            "Clients": [1, 3, 2],
        }
    )

    out = calculate_concurrent_clients(df)
    wk0 = out.filter(pl.col("current_week") == 0).row(0, named=True)
    assert abs(wk0["avg_clients"] - 2.5) < 1e-9  # peaks 3 and 2
    assert wk0["max_clients"] == 3
    assert "week_date" in out.columns


def test_feature_adoption_weekly_share_per_flag_treats_null_as_off():
    df = pl.DataFrame(
        {
            "UserID": [1, 2],
            "current_week": [0, 0],
            "HasActions": [True, False],
            "HasHostname": [False, False],
            "HasCustomAddress": [False, False],
            "HasCustomBase": [False, False],
            "HasShell": [None, True],  # null (old data) must count as not adopted
        }
    )

    out = calculate_feature_adoption(df)
    by_feature = {r["feature"]: r for r in out.iter_rows(named=True)}
    assert abs(by_feature["HasActions"]["adoption"] - 0.5) < 1e-9
    assert abs(by_feature["HasShell"]["adoption"] - 0.5) < 1e-9  # only user 2
    assert "week_date" in out.columns


def test_browser_mix_returns_browser_and_os_distributions():
    attrs = pl.DataFrame(
        {
            "UserID": [1, 2, 3],
            "browser_family": ["Chrome", "Chrome", "Firefox"],
            "os_family": ["Windows", "macOS", "Windows"],
        }
    )

    browser_df, os_df = calculate_browser_mix(attrs)
    b = {r["browser_family"]: r for r in browser_df.iter_rows(named=True)}
    o = {r["os_family"]: r for r in os_df.iter_rows(named=True)}
    assert b["Chrome"]["installs"] == 2
    assert abs(b["Chrome"]["share"] - 2 / 3) < 1e-9
    assert o["Windows"]["installs"] == 2
    assert abs(o["macOS"]["share"] - 1 / 3) < 1e-9
