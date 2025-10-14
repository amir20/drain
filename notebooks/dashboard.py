"""Streamlit dashboard for Dozzle retention analysis."""

import streamlit as st

from config import PAGE_TITLE, PAGE_LAYOUT, COHORT_DETAILS_HEAD
from data_loader import load_and_process_data
from cohort_analysis import (
    compute_cohort_data,
    calculate_cohort_retention,
    prepare_retention_matrix,
)
from usage_analysis import calculate_usage_frequency
from engagement_analysis import (
    calculate_user_lifecycle_metrics,
    calculate_stickiness_metrics,
    calculate_engagement_depth,
    calculate_cohort_engagement_metrics,
)
from visualizations import (
    display_retention_heatmap,
    display_usage_frequency_analysis,
    display_user_lifecycle_analysis,
    display_stickiness_analysis,
    display_engagement_depth_analysis,
    display_cohort_engagement_analysis,
)


# Configuration
st.set_page_config(page_title=PAGE_TITLE, layout=PAGE_LAYOUT)


def main() -> None:
    """Main dashboard function."""
    st.title("Cohort Retention Analysis")

    # Load and process data
    with st.spinner("Loading and processing data..."):
        df = load_and_process_data()
        df = compute_cohort_data(df)

    # Calculate cohort retention
    with st.spinner("Calculating cohort retention rates..."):
        cohort_counts = calculate_cohort_retention(df)
        retention = prepare_retention_matrix(cohort_counts)

    # Display cohort retention heatmap
    with st.spinner("Generating retention heatmap..."):
        display_retention_heatmap(retention)

    # Show data details
    with st.expander(f"Show top {COHORT_DETAILS_HEAD} rows"):
        st.dataframe(cohort_counts.head(COHORT_DETAILS_HEAD))

    # Calculate and display usage frequency analysis
    with st.spinner("Calculating usage frequency metrics..."):
        usage_frequency, overall_avg = calculate_usage_frequency(df)
        display_usage_frequency_analysis(usage_frequency, overall_avg)

    # Calculate and display user lifecycle metrics
    with st.spinner("Calculating user lifecycle metrics..."):
        lifecycle_df = calculate_user_lifecycle_metrics(df)
        display_user_lifecycle_analysis(lifecycle_df)

    # Calculate and display stickiness metrics
    with st.spinner("Calculating stickiness metrics..."):
        stickiness_df, stickiness_stats = calculate_stickiness_metrics(df)
        display_stickiness_analysis(stickiness_df, stickiness_stats)

    # Calculate and display engagement depth
    with st.spinner("Calculating engagement depth metrics..."):
        engagement_depth_df = calculate_engagement_depth(df)
        display_engagement_depth_analysis(engagement_depth_df)

    # Calculate and display cohort engagement metrics
    with st.spinner("Calculating cohort engagement metrics..."):
        cohort_engagement_df = calculate_cohort_engagement_metrics(df)
        display_cohort_engagement_analysis(cohort_engagement_df)


if __name__ == "__main__":
    main()
