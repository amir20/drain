# Dozzle Analytics Dashboard

A comprehensive user behavior analytics dashboard for Dozzle logs, providing deep insights into user engagement, retention, growth patterns, and churn analysis.

## 🌟 Features

### 📊 **Key Metrics Overview**
- Total users and events tracking
- Average events per user
- Data range and coverage statistics

### 📈 **Growth Analysis**
- Active users over time tracking
- New vs returning user segmentation
- Growth rate calculations and trends
- Event volume analysis with forecasting

### 🎯 **Engagement Analysis**
- Hourly activity patterns - identify peak usage times
- Day-of-week usage analysis - understand weekly patterns
- User segmentation (Power, Regular, Casual, New users)
- Interactive engagement visualizations

### 🔄 **Retention Analysis**
- Cohort retention heatmaps with 15 cohorts over 12 weeks
- Retention rate calculations and insights
- Best performing cohort identification
- Product-market fit indicators

### 🔬 **Advanced Analytics**
- Churn risk analysis and predictions
- User value segmentation (High, Medium, Low value)
- Risk matrices for proactive user management
- Advanced behavioral insights

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Streamlit
- Polars
- Plotly
- NumPy

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd drain/notebooks
   ```

2. **Install dependencies:**
   ```bash
   pip install streamlit polars plotly numpy pandas
   ```

3. **Prepare your data:**
   - Place your Dozzle parquet files in `./data/` directory
   - Files should follow the pattern: `day-*.parquet`

### Running the Dashboard

**Option 1: Modular Version (Recommended)**
```bash
streamlit run dashboard_refactored.py
```

**Option 2: Single File Version**
```bash
streamlit run dashboard.py
```

The dashboard will be available at `http://localhost:8501`

## 📁 Project Structure

```
notebooks/
├── dashboard.py                    # Original single-file dashboard
├── dashboard_refactored.py         # Main entry point (modular)
├── dashboard_modules/              # Modular components
│   ├── __init__.py                # Package initialization
│   ├── config.py                  # Configuration and constants
│   ├── data_processing.py         # Data loading and calculations
│   └── visualizations.py          # Display and plotting functions
├── data/                          # Data directory (parquet files)
│   └── day-*.parquet             # Dozzle log data
└── README.md                      # This file
```

## 🔧 Configuration

### Data Configuration
Edit `dashboard_modules/config.py` to customize:

```python
# Data paths and baseline date
BASELINE = datetime(year=2020, month=1, day=1)
DATA_PATH = "./data/day-*.parquet"

# User segmentation thresholds
USER_SEGMENTS = {
    "power_user": 100,    # 100+ events
    "regular_user": 50,   # 50+ events
    "casual_user": 10     # 10+ events
}

# Churn analysis thresholds (days)
CHURN_THRESHOLDS = {
    "active": 7,      # Active if used within 7 days
    "at_risk": 30,    # At risk if 7-30 days inactive
    "churning": 90    # Churning if 30-90 days inactive
}
```

### Visual Customization
Modify colors, chart types, and styling in `config.py`:

```python
COLORS = {
    "primary": "#3b82f6",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#ef4444"
}
```

## 📊 Dashboard Sections

### 1. Growth Tab
- **Active Users Timeline**: Track user growth over time
- **New vs Returning Users**: Understand user acquisition patterns
- **Growth Rates**: Monitor week-over-week growth trends
- **Event Volume**: Analyze overall platform usage

### 2. Engagement Tab
- **Hourly Activity**: Identify peak usage hours
- **Weekly Patterns**: Understand day-of-week trends
- **User Segments**: Visualize user distribution by engagement level
- **Behavioral Insights**: Deep-dive into user patterns

### 3. Retention Tab
- **Cohort Heatmap**: Visual retention analysis over 12 weeks
- **Retention Insights**: Key metrics and best-performing cohorts
- **Trend Analysis**: Long-term retention patterns

### 4. Advanced Tab
- **Churn Analysis**: Risk assessment by user value
- **Predictive Insights**: Identify at-risk high-value users
- **Action Items**: Data-driven recommendations

## 🔍 Data Requirements

### Expected Data Schema
Your parquet files should contain these columns:

| Column | Type | Description |
|--------|------|-------------|
| `Name` | String | Event name (filter for "events") |
| `CreatedAt` | Datetime | Event timestamp |
| `ServerID` | String | Server identifier (optional) |
| `RemoteIP` | String | User IP address |
| `Browser` | String | Browser information |
| ... | ... | Other Dozzle log fields |

### Data Processing
The dashboard automatically:
- Filters for events with `Name == "events"`
- Creates unique `UserID` from `ServerID` or `RemoteIP` hash
- Calculates cohorts based on first user activity
- Generates time-based metrics (hourly, daily, weekly)

## 🎨 Customization

### Adding New Metrics
1. Add calculation functions to `data_processing.py`
2. Create visualization functions in `visualizations.py`
3. Update the main dashboard in `dashboard_refactored.py`

### Styling Changes
- Modify `CUSTOM_CSS` in `config.py` for global styling
- Update color schemes in `CHART_COLORS` configuration
- Customize layout in visualization functions

### Performance Optimization
- Data is cached for 5 minutes by default (`CACHE_TTL`)
- Large datasets are automatically paginated
- Efficient Polars operations for fast processing

## 🐛 Troubleshooting

### Common Issues

**1. "No data files found"**
- Ensure parquet files are in `./data/` directory
- Check file naming pattern: `day-*.parquet`

**2. "Column not found" errors**
- Verify data schema matches expected format
- Check that `Name` column contains "events"

**3. Empty visualizations**
- Ensure data contains the required date range
- Check that `CreatedAt` timestamps are valid

**4. Performance issues**
- Reduce `RETENTION_MATRIX_CONFIG` cohort/week counts
- Increase `CACHE_TTL` for slower data updates
- Consider data sampling for very large datasets

### Debug Mode
Enable debug information by adding to your environment:
```bash
export STREAMLIT_LOGGER_LEVEL=debug
streamlit run dashboard_refactored.py
```

## 📈 Analytics Insights

### Key Metrics to Monitor
- **Week 1 Retention**: Indicates product-market fit
- **Power User Growth**: High-value user acquisition
- **Churn Risk**: Early warning for user loss
- **Peak Usage Times**: Infrastructure planning

### Business Applications
- **Product Development**: Feature prioritization based on usage
- **Marketing**: Optimal timing for campaigns
- **Support**: Resource allocation during peak hours
- **Growth Strategy**: User acquisition and retention focus

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black dashboard_modules/
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Streamlit** - For the amazing dashboard framework
- **Polars** - For lightning-fast data processing
- **Plotly** - For interactive visualizations
- **Dozzle Community** - For the inspiration and use case

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/your-repo/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-repo/discussions)
- **Documentation**: [Wiki Pages](https://github.com/your-repo/wiki)

---

Built with ❤️ for the Dozzle community