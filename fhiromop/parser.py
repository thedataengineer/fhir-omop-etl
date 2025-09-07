"""Utility functions for reading and flattening FHIR NDJSON files.

This module contains helpers to load FHIR resources exported by Synthea in
NDJSON (newline delimited JSON) format and recursively flatten nested
structures into a flat :class:`pyspark.sql.DataFrame`.

Only a very small subset of the FHIR specification is supported and the
flattening logic is intentionally simple.  The goal is to provide an easy to
understand example that can be extended for additional resources.
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

__all__ = ["read_patient_ndjson", "flatten"]


def read_patient_ndjson(spark: SparkSession, path: str) -> DataFrame:
    """Read a Synthea FHIR Patient NDJSON file and return a flat DataFrame.

    Parameters
    ----------
    spark:
        Active :class:`~pyspark.sql.SparkSession`.
    path:
        Path to the NDJSON file containing Patient resources.

    Returns
    -------
    DataFrame
        A DataFrame where nested structures have been flattened into top level
        columns using ``_`` as a separator.
    """
    df = spark.read.json(path)
    return flatten(df)


def flatten(df: DataFrame) -> DataFrame:
    """Recursively flatten a DataFrame with nested structures.

    The algorithm iteratively expands ``StructType`` columns into individual
    columns and explodes arrays of structs.  Arrays of primitive values are
    converted to comma separated strings.  The function stops once no complex
    fields remain.

    Parameters
    ----------
    df:
        Input DataFrame possibly containing nested columns.

    Returns
    -------
    DataFrame
        A flattened DataFrame with no ``StructType`` or ``ArrayType`` columns.
    """
    complex_fields: List[tuple[str, T.DataType]] = [
        (field.name, field.dataType)
        for field in df.schema.fields
        if isinstance(field.dataType, (T.ArrayType, T.StructType))
    ]

    while complex_fields:
        col_name, data_type = complex_fields.pop(0)
        if isinstance(data_type, T.StructType):
            # Expand struct by adding each field as a top level column
            expanded = [
                F.col(f"{col_name}.{name}").alias(f"{col_name}_{name}")
                for name in data_type.names
            ]
            df = df.select("*", *expanded).drop(col_name)
        elif isinstance(data_type, T.ArrayType):
            if isinstance(data_type.elementType, T.StructType):
                # Explode array of structs and continue flattening
                df = df.withColumn(col_name, F.explode_outer(col_name))
            else:
                # Array of primitives -> comma separated string
                df = df.withColumn(
                    col_name, F.concat_ws(",", col_name)
                )
        complex_fields = [
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, (T.ArrayType, T.StructType))
        ]
    return df
