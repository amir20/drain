"""Streamlit dashboard for Dozzle retention and usage analysis."""

import polars as pl
import streamlit as st
from attribute_analysis import (
    CONTAINER_BUCKETS,
    FEATURE_FLAGS,
    calculate_auth_mix,
    calculate_browser_mix,
    calculate_concurrent_clients,
    calculate_deployment_scale,
    calculate_feature_adoption,
    calculate_install_attributes,
    calculate_new_installs,
    calculate_version_adoption,
    filter_installs,
)
from cohort_analysis import (
    calculate_cohort_retention,
    compute_cohort_data,
    prepare_retention_matrix,
)
from config import COHORT_DETAILS_HEAD, PAGE_LAYOUT, PAGE_TITLE
from data_loader import (
    calculate_identity_quality,
    load_and_process_data,
    load_start_beacons,
)
from engagement_analysis import (
    calculate_cohort_engagement_metrics,
    calculate_engagement_depth,
    calculate_stickiness_metrics,
    calculate_user_lifecycle_metrics,
)
from usage_analysis import calculate_usage_frequency
from visualizations import (
    display_auth_mix_analysis,
    display_browser_mix_analysis,
    display_cohort_engagement_analysis,
    display_concurrent_clients_analysis,
    display_deployment_scale_analysis,
    display_engagement_depth_analysis,
    display_feature_adoption_analysis,
    display_new_installs_analysis,
    display_retention_heatmap,
    display_stickiness_analysis,
    display_usage_frequency_analysis,
    display_user_lifecycle_analysis,
    display_version_adoption_analysis,
)

# Telemetry columns present in the schema but empty/zero across the entire history.
DEAD_TELEMETRY_COLUMNS = [
    "ServerVersion",
    "SubCommand",
    "Mode (swarm)",
    "RemoteAgents",
    "RemoteClients",
    "FilterLength",
    "IsSwarmMode",
]

st.set_page_config(page_title=PAGE_TITLE, layout=PAGE_LAYOUT)


@st.cache_resource(ttl=3600)
def get_events() -> pl.DataFrame:
    """Load the events beacons once. Cached as a large read-only resource."""
    return load_and_process_data()


@st.cache_resource(ttl=3600)
def get_cohort_full() -> pl.DataFrame:
    """Cohort-computed events for the unsegmented (whole-population) view."""
    return compute_cohort_data(get_events())


@st.cache_resource(ttl=3600)
def get_install_attributes() -> pl.DataFrame:
    """Per-install latest attributes, used for distributions and segmentation."""
    return calculate_install_attributes(get_events())


@st.cache_resource(ttl=3600)
def get_starts() -> pl.DataFrame:
    """Start (app-launch) beacons for new-install analysis."""
    return load_start_beacons()


def _options(attrs: pl.DataFrame, column: str, limit: int | None = None) -> list:
    counts = attrs[column].drop_nulls().value_counts(sort=True)[column].to_list()
    return counts[:limit] if limit else counts


def build_segment_selections(attrs: pl.DataFrame) -> dict:
    """Render sidebar segmentation controls and return the active selections."""
    st.sidebar.header("Segment installs")
    st.sidebar.caption("Filters re-slice every chart by install attributes.")

    selections: dict = {}
    version = st.sidebar.multiselect("Version", _options(attrs, "Version", limit=30))
    if version:
        selections["Version"] = version

    auth = st.sidebar.multiselect("Auth provider", _options(attrs, "AuthProvider"))
    if auth:
        selections["AuthProvider"] = auth

    size = st.sidebar.multiselect("Deployment size", CONTAINER_BUCKETS)
    if size:
        selections["container_bucket"] = size

    browser = st.sidebar.multiselect("Browser", _options(attrs, "browser_family"))
    if browser:
        selections["browser_family"] = browser

    os_family = st.sidebar.multiselect("OS", _options(attrs, "os_family"))
    if os_family:
        selections["os_family"] = os_family

    require = st.sidebar.multiselect("Require features", FEATURE_FLAGS)
    for flag in require:
        selections[flag] = [True]

    return selections


def main() -> None:
    """Main dashboard function."""
    st.title("Dozzle Usage & Retention Analysis")

    with st.spinner("Loading and processing data..."):
        events = get_events()
        attrs = get_install_attributes()

    selections = build_segment_selections(attrs)
    if selections:
        ids = filter_installs(attrs, selections)
        if ids.len() == 0:
            st.warning("No installs match the current segment. Adjust the sidebar filters.")
            return
        id_set = ids.implode()  # implode -> unambiguous membership test (polars #22149)
        df = compute_cohort_data(events.filter(pl.col("UserID").is_in(id_set)))
        attrs = attrs.filter(pl.col("UserID").is_in(id_set))
        starts = get_starts().filter(pl.col("UserID").is_in(id_set))
        st.sidebar.success(f"Segment: {ids.len():,} installs")
    else:
        df = get_cohort_full()
        starts = get_starts()

    overview, retention_tab, usage_tab, lifecycle_tab, deploy_tab, version_tab = st.tabs(
        ["Overview", "Retention", "Usage & Stickiness", "Lifecycle", "Deployments", "Versions"]
    )

    quality = calculate_identity_quality(df)
    new_installs = calculate_new_installs(starts)
    stickiness_df, stickiness_stats = calculate_stickiness_metrics(df)

    with overview:
        _render_overview(quality, stickiness_stats, new_installs)

    with retention_tab:
        cohort_counts = calculate_cohort_retention(df)
        display_retention_heatmap(prepare_retention_matrix(cohort_counts))
        with st.expander(f"Show top {COHORT_DETAILS_HEAD} rows"):
            st.dataframe(cohort_counts.head(COHORT_DETAILS_HEAD))
        display_cohort_engagement_analysis(calculate_cohort_engagement_metrics(df))

    with usage_tab:
        usage_frequency, overall_avg = calculate_usage_frequency(df)
        display_usage_frequency_analysis(usage_frequency, overall_avg)
        display_stickiness_analysis(stickiness_df, stickiness_stats)
        display_concurrent_clients_analysis(calculate_concurrent_clients(df))
        display_engagement_depth_analysis(calculate_engagement_depth(df))

    with lifecycle_tab:
        display_user_lifecycle_analysis(calculate_user_lifecycle_metrics(df))
        display_new_installs_analysis(new_installs)

    with deploy_tab:
        display_deployment_scale_analysis(calculate_deployment_scale(attrs))
        display_auth_mix_analysis(calculate_auth_mix(df))
        browser_df, os_df = calculate_browser_mix(attrs)
        display_browser_mix_analysis(browser_df, os_df)

    with version_tab:
        display_version_adoption_analysis(calculate_version_adoption(df))
        display_feature_adoption_analysis(calculate_feature_adoption(df))


def _render_overview(quality: dict, stickiness_stats: dict, new_installs: pl.DataFrame) -> None:
    """Top-level KPIs plus data-quality caveats."""
    st.header("Overview")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Installs", f"{quality['total_users']:,}")
    col2.metric("Current WAU", f"{stickiness_stats['current_wau']:,}")
    col3.metric("Current MAU (4-week)", f"{stickiness_stats['current_mau']:,}")
    if new_installs.height:
        col4.metric("New Installs (Last Week)", f"{new_installs['new_installs'][-1]:,}")

    st.caption(
        f"{quality['total_users']:,} installs · {quality['total_rows']:,} events. "
        f"{quality['ip_user_pct']:.1%} of installs are identified by IP address "
        "(no ServerID); their retention and churn are approximate, since IPs change "
        "over time and are shared behind NAT."
    )

    with st.expander("Data quality notes"):
        st.markdown(
            f"- **IP-identified installs:** {quality['ip_users']:,} of {quality['total_users']:,} "
            f"({quality['ip_user_pct']:.1%}) lack a stable ServerID and fall back to a hashed "
            "RemoteIP. These inflate new-user/churn counts and deflate retention.\n"
            "- **Engagement = beacon activity**, not feature use: `events` are periodic "
            "telemetry beacons, so per-user event counts mostly reflect uptime/frequency.\n"
            "- **Unused telemetry:** these columns are collected but empty/zero across the "
            f"entire history, so nothing can be charted from them yet: "
            f"{', '.join('`' + c + '`' for c in DEAD_TELEMETRY_COLUMNS)}."
        )


if __name__ == "__main__":
    main()
