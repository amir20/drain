"""
Dashboard Modules Package

A modular analytics dashboard for Dozzle user behavior analysis.

This package contains:
- config: Configuration constants and styling
- data_processing: Data loading and calculation functions
- visualizations: Display and plotting functions
"""

__version__ = "1.0.0"
__author__ = "Dozzle Analytics Team"

from .config import *
from .data_processing import *
from .visualizations import *

__all__ = [
    # Configuration
    "BASELINE",
    "DATA_PATH",
    "CACHE_TTL",
    "COLORS",
    "CUSTOM_CSS",
    "USER_SEGMENTS",
    "CHURN_THRESHOLDS",
    "VALUE_SEGMENTS",
    "WEEKDAY_MAP",
    # Data Processing Functions
    "load_and_process_data",
    "compute_cohort_data",
    "calculate_cohort_retention",
    "calculate_growth_metrics",
    "calculate_engagement_metrics",
    "calculate_churn_metrics",
    "prepare_retention_matrix",
    # Visualization Functions
    "apply_custom_styling",
    "display_header",
    "display_key_metrics",
    "display_growth_analysis",
    "display_engagement_analysis",
    "display_retention_heatmap",
    "display_churn_analysis",
    "display_sidebar",
    "display_footer",
    "display_data_tables",
]
