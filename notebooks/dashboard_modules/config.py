"""
Configuration module for the Dozzle Analytics Dashboard.

Contains constants, styling configurations, and global settings.
"""

from datetime import datetime, timezone

# Data Configuration
BASELINE = datetime(year=2020, month=1, day=1).replace(tzinfo=timezone.utc)
DATA_PATH = "./data/day-*.parquet"

# Cache Configuration
CACHE_TTL = 300  # 5 minutes

# UI Configuration
PAGE_TITLE = "Dozzle Analytics Dashboard"
PAGE_ICON = "📊"
LAYOUT = "wide"
SIDEBAR_STATE = "expanded"

# Color Schemes
COLORS = {
    "primary": "#3b82f6",
    "secondary": "#6366f1",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#ef4444",
    "gradient_start": "#667eea",
    "gradient_end": "#764ba2",
}

# Custom CSS Styling
CUSTOM_CSS = """
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 2rem;
        text-align: center;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }
    .insight-box {
        background-color: #f8fafc;
        border-left: 4px solid #3b82f6;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
</style>
"""

# Chart Configuration
CHART_COLORS = {
    "retention_heatmap": "RdYlGn",
    "activity_bars": "viridis",
    "weekly_activity": "plasma",
    "pie_segments": "Set3",
    "churn_heatmap": "Reds",
}

# User Segmentation Thresholds
USER_SEGMENTS = {"power_user": 100, "regular_user": 50, "casual_user": 10}

# Churn Analysis Thresholds (in days)
CHURN_THRESHOLDS = {"active": 7, "at_risk": 30, "churning": 90}

# Value Segmentation Thresholds
VALUE_SEGMENTS = {"high_value": 100, "medium_value": 20}

# Display Configuration
RETENTION_MATRIX_CONFIG = {"cohort_count": 15, "week_count": 12}

# Day of Week Mapping (Polars uses 1-7)
WEEKDAY_MAP = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}

# Menu Items
MENU_ITEMS = {
    "About": "Dozzle Analytics Dashboard - Comprehensive user behavior analysis"
}
