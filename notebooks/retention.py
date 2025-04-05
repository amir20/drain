# %%
import polars as pl
import streamlit as st
import glob
from datetime import datetime, timedelta, timezone


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
value_counts = df.group_by("cohort_index").agg(pl.count().alias("count"))
value_counts = value_counts.sort("count", descending=True)
st.write("### Frequency of Values in 'current_week'")
st.bar_chart(value_counts.to_pandas().set_index("cohort_index"))
