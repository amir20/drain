import polars as pl
import streamlit as st
import glob
from datetime import datetime, timedelta, timezone
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(page_title="Dozzle Retention Analysis", layout="wide")

# rsync -avz -e "ssh -o RemoteCommand=none" "b.dozzle.dev:/data/beacon/day*.parquet" ./data/
parquet_files = glob.glob("./data/day-*.parquet")
df = pl.concat([pl.read_parquet(file) for file in parquet_files], how="diagonal")

df = (
    df.filter(df["Name"] == "events")
    .with_columns(
        pl.when(pl.col("ServerID").is_null() | (pl.col("ServerID") == ""))
        .then(pl.col("RemoteIP").hash())
        .otherwise(pl.col("ServerID").hash())
        .alias("UserID")
    )
    .drop(
        [
            "RemoteIP",
            "HasHostname",
            "FilterLength",
            "Clients",
            "HasActions",
            "Browser",
            "ServerID",
            "HasCustomAddress",
            "HasCustomBase",
            "IsSwarmMode",
            "RemoteClients",
            "RemoteAgents",
        ]
    )
)

# Compute cohort
df = df.with_columns(pl.col("CreatedAt").min().over("UserID").alias("activated"))
baseline = datetime(year=2020, month=1, day=1).replace(tzinfo=timezone.utc)
df = df.with_columns(
    [
        # Calculate activated_week
        ((pl.col("activated") - pl.lit(baseline)) / timedelta(weeks=1))
        .cast(pl.Int64)
        .alias("activated_week"),
        # Calculate current_week
        ((pl.col("CreatedAt") - pl.lit(baseline)) / timedelta(weeks=1))
        .cast(pl.Int64)
        .alias("current_week"),
    ]
)

# Add cohort_index as the difference between weeks
df = df.with_columns(
    (pl.col("current_week") - pl.col("activated_week")).alias("cohort_index")
)


# Count unique values in 'current_week' column
value_counts = df.group_by("cohort_index").agg(pl.len().alias("count"))
value_counts = value_counts.sort("count", descending=True)

# Group by and count unique users
cohort_counts = df.group_by(["activated_week", "cohort_index"]).agg(
    pl.col("UserID").n_unique().alias("users")
)


# Sort by activated_week
cohort_counts = cohort_counts.sort("activated_week")

# Calculate retention rate
cohort_counts = cohort_counts.with_columns(
    [
        (pl.col("users") / pl.col("users").max().over("activated_week")).alias(
            "retention_rate"
        ),
        (
            pl.lit(baseline)
            + pl.col("activated_week").cast(pl.Int64) * pl.duration(weeks=1)
        ).alias("activated_date"),
    ]
)

with st.expander("Show top 50 rows"):
    st.dataframe(cohort_counts.head(50))

retention = (
    cohort_counts.pivot(
        on="cohort_index",
        index="activated_date",
        values="retention_rate",
        aggregate_function="first",
        sort_columns=True,
    )
    .tail(10)
    .select(["activated_date"] + [str(i) for i in range(8)])
)


def display_retention_heatmap(retention):
    # Convert Polars DataFrame to Pandas (Plotly works better with pandas)
    retention_pd = retention.to_pandas()

    # Set activated_date as index if it's not already
    if "activated_date" in retention_pd.columns:
        retention_pd = retention_pd.set_index("activated_date")

    # Create a heatmap using Plotly
    fig = px.imshow(
        retention_pd,
        labels=dict(x="Week", y="Cohort", color="Retention Rate"),
        x=retention_pd.columns,
        y=retention_pd.index,
        color_continuous_scale="Viridis",
        zmin=0,
        zmax=1,  # Assuming retention rates are between 0 and 1
        aspect="auto",
        text_auto=".0%",  # Format as percentage
    )

    fig_go = go.Figure(fig)
    # Add gaps between cells
    fig_go.data[0].xgap = 2
    fig_go.data[0].ygap = 2

    # Customize layout
    fig_go.update_layout(
        title="Cohort Retention Analysis",
        xaxis_title="Week Number",
        yaxis_title="Cohort Start Date",
        coloraxis_colorbar=dict(title="Retention Rate"),
        height=600,
        width=900,
    )

    # Display in Streamlit
    st.plotly_chart(fig_go, use_container_width=True)


# In your Streamlit app
st.title("Cohort Retention Analysis")
display_retention_heatmap(retention)


# Add this after your cohort_counts calculation and before the expander

# Calculate average usage frequency per week
usage_frequency = (
    df.group_by(["UserID", "current_week"])
    .agg(pl.len().alias("events_count"))
    .group_by("current_week")
    .agg(
        pl.col("events_count").mean().alias("avg_events_per_user_per_week"),
        pl.col("UserID").n_unique().alias("active_users"),
    )
)

# Add the date for better readability
usage_frequency = usage_frequency.with_columns(
    (
        pl.lit(baseline) + pl.col("current_week").cast(pl.Int64) * pl.duration(weeks=1)
    ).alias("week_date")
).sort("current_week")

# Overall average (across all time)
overall_avg = (
    df.group_by(["UserID", "current_week"])
    .agg(pl.len().alias("events_count"))
    .select(pl.col("events_count").mean().alias("overall_avg_events_per_user_per_week"))
)

st.header("Usage Frequency Analysis")
col1, col2 = st.columns(2)

with col1:
    st.metric(
        "Overall Average Events per User per Week",
        f"{overall_avg['overall_avg_events_per_user_per_week'][0]:.2f}",
    )

with col2:
    # Recent average (last 4 weeks)
    recent_avg = usage_frequency.tail(4).select(
        pl.col("avg_events_per_user_per_week").mean()
    )[0, 0]
    st.metric("Recent Average (Last 4 Weeks)", f"{recent_avg:.2f}")

# Display usage frequency over time
fig_usage = px.line(
    usage_frequency.to_pandas(),
    x="week_date",
    y="avg_events_per_user_per_week",
    title="Average Events per User per Week Over Time",
    labels={"week_date": "Week", "avg_events_per_user_per_week": "Avg Events per User"},
)
fig_usage.update_traces(mode="lines+markers")
st.plotly_chart(fig_usage, use_container_width=True)

# Show detailed table
with st.expander("Show Usage Frequency Details"):
    st.dataframe(
        usage_frequency.select(
            ["week_date", "avg_events_per_user_per_week", "active_users"]
        ).tail(20)
    )
