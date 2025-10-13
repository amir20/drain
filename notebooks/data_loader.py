"""Data loading and processing utilities for retention analysis."""

import glob
import polars as pl

from config import DATA_PATH


def load_and_process_data() -> pl.DataFrame:
    """Load parquet files and perform initial data processing.

    Returns:
        pl.DataFrame: Processed dataframe with UserID and cleaned columns.
    """
    parquet_files = glob.glob(DATA_PATH)
    df = pl.concat([pl.read_parquet(file) for file in parquet_files], how="diagonal")

    # Filter and clean data
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

    return df
