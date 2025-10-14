"""Configuration constants for the Dozzle Retention Analysis dashboard."""

from datetime import datetime, timezone

# Baseline date for cohort analysis
BASELINE = datetime(year=2020, month=1, day=1).replace(tzinfo=timezone.utc)

# Data file path pattern
DATA_PATH = "./data/day-*.parquet"

# Dashboard configuration
PAGE_TITLE = "Dozzle Retention Analysis"
PAGE_LAYOUT = "wide"

# Visualization settings
HEATMAP_HEIGHT = 600
HEATMAP_WIDTH = 900
HEATMAP_COLOR_SCALE = "Viridis"
HEATMAP_GAP = 2

# Analysis parameters
RECENT_WEEKS_COUNT = 4
RETENTION_MATRIX_TAIL = 10
RETENTION_MATRIX_WEEKS = 8
USAGE_DETAILS_TAIL = 20
COHORT_DETAILS_HEAD = 50
