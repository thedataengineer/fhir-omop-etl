"""Functions for writing OMOP formatted data."""
from __future__ import annotations

from pyspark.sql import DataFrame

__all__ = ["save_person"]


def save_person(df: DataFrame, path: str) -> None:
    """Save a PERSON table DataFrame to a CSV file.

    The implementation collects the data on the driver node and writes a single
    CSV file.  This is sufficient for demonstration purposes and small sample
    data sets.  In a production ETL one would typically write directly using
    Spark's distributed ``DataFrameWriter``.
    """
    df.toPandas().to_csv(path, index=False)
