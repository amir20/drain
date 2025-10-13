"""
Dozzle Analytics Dashboard - Main Application

A comprehensive user behavior analytics dashboard for Dozzle logs.
Provides insights into user engagement, retention, growth, and churn patterns.
"""

import streamlit as st
from dashboard_modules.config import (
    PAGE_TITLE,
    PAGE_ICON,
    LAYOUT,
    SIDEBAR_STATE,
    MENU_ITEMS,
)
from dashboard_modules.data_processing import (
    load_and_process_data,
    compute_cohort_data,
    calculate_cohort_retention,
    calculate_growth_metrics,
    calculate_engagement_metrics,
    calculate_churn_metrics,
    prepare_retention_matrix,
)
from dashboard_modules.visualizations import (
    apply_custom_styling,
    display_header,
    display_key_metrics,
    display_growth_analysis,
    display_engagement_analysis,
    display_retention_heatmap,
    display_churn_analysis,
    display_sidebar,
    display_footer,
    display_data_tables,
)


def configure_page():
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout=LAYOUT,
        initial_sidebar_state=SIDEBAR_STATE,
        menu_items=MENU_ITEMS,
    )


def load_data():
    """Load and process all required data."""
    with st.spinner("🔄 Loading and processing data..."):
        df = load_and_process_data()

        if df.is_empty():
            st.error("No data available to display.")
            return None

        df = compute_cohort_data(df)
        return df


def calculate_metrics(df):
    """Calculate all analytics metrics."""
    with st.spinner("📊 Calculating metrics..."):
        growth_metrics = calculate_growth_metrics(df)
        engagement_metrics = calculate_engagement_metrics(df)
        churn_metrics = calculate_churn_metrics(df)
        cohort_counts = calculate_cohort_retention(df)
        retention = prepare_retention_matrix(cohort_counts)

        return {
            "growth": growth_metrics,
            "engagement": engagement_metrics,
            "churn": churn_metrics,
            "cohort_counts": cohort_counts,
            "retention": retention,
        }


def display_dashboard(metrics):
    """Display the main dashboard interface."""
    # Apply styling and display header
    apply_custom_styling()
    display_header()

    # Display key metrics
    display_key_metrics(metrics["growth"])

    # Create tabs for better organization
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 Growth", "🎯 Engagement", "🔄 Retention", "🔬 Advanced"]
    )

    with tab1:
        display_growth_analysis(metrics["growth"])

    with tab2:
        display_engagement_analysis(metrics["engagement"])

    with tab3:
        display_retention_heatmap(metrics["retention"])

    with tab4:
        display_churn_analysis(metrics["churn"])

    # Display expandable data tables
    display_data_tables(
        metrics["growth"], metrics["engagement"], metrics["cohort_counts"]
    )


def main():
    """Main application function."""
    # Configure page
    configure_page()

    # Display sidebar controls
    sidebar_config = display_sidebar()

    # Load and process data
    df = load_data()
    if df is None:
        return

    # Calculate all metrics
    metrics = calculate_metrics(df)

    # Display dashboard
    display_dashboard(metrics)

    # Display footer
    display_footer()


if __name__ == "__main__":
    main()
