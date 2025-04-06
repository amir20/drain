import polars as pl
import streamlit as st
import glob
from datetime import datetime, timedelta, timezone
import plotly.express as px

st.set_page_config(layout="wide")

parquet_files = glob.glob("./data/day-*.parquet")
df = pl.concat([pl.read_parquet(file) for file in parquet_files], how="diagonal")
st.title("Retention Analysis Dozzle")

# Only look for event types
df = df.filter(df["Name"] == "events")

# Generate hash from ServerID
df = df.with_columns(
    pl.when(pl.col("ServerID").is_null() | (pl.col("ServerID") == ""))
    .then(pl.col("RemoteIP").hash())
    .otherwise(pl.col("ServerID").hash())
    .alias("UserID")
)

df = df.drop(
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
    .select(["activated_date", "0", "1", "2", "3", "4", "5", "6", "7"])
)

st.write("### Retention Rate")


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

    # Customize layout
    fig.update_layout(
        title="Cohort Retention Analysis",
        xaxis_title="Week Number",
        yaxis_title="Cohort Start Date",
        coloraxis_colorbar=dict(title="Retention Rate"),
        height=600,
        width=900,
    )

    # Display in Streamlit
    st.plotly_chart(fig, use_container_width=True)


# In your Streamlit app
st.title("Cohort Retention Analysis")
display_retention_heatmap(retention)
