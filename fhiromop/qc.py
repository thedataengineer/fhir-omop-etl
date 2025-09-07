"""Simple data quality checks for ETL outputs."""
from __future__ import annotations

from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame, functions as F

__all__ = ["row_count_matches", "missing_counts", "run_checks"]


def row_count_matches(df: DataFrame, sample_csv: str) -> bool:
    """Compare row count against a reference CSV file.

    Parameters
    ----------
    df:
        DataFrame to evaluate.
    sample_csv:
        Path to reference OMOP CSV (e.g. Synthea output).

    Returns
    -------
    bool
        ``True`` if the row counts match.  If the reference file is missing or
        empty the function returns ``True`` when ``df`` is also empty.
    """
    try:
        sample = pd.read_csv(sample_csv)
        sample_count = len(sample)
    except Exception:
        sample_count = 0
    return df.count() == sample_count


def missing_counts(df: DataFrame) -> Dict[str, int]:
    """Return the number of null values for each column in ``df``."""
    return {c: df.filter(F.col(c).isNull()).count() for c in df.columns}


def run_checks(df: DataFrame, sample_csv: str) -> Dict[str, object]:
    """Run basic quality checks returning a summary dictionary."""
    return {
        "row_count_match": row_count_matches(df, sample_csv),
        "missing": missing_counts(df),
    }
