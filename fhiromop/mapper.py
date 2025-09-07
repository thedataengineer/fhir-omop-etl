"""Mapping logic to transform flattened FHIR data to OMOP tables."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, functions as F

from .config import FieldMapping, MappingConfig

__all__ = ["map_person"]


def _apply_field(df: DataFrame, mapping: FieldMapping) -> F.Column:
    """Build a Spark column expression for a single field mapping."""
    if mapping.source is not None:
        col = F.col(mapping.source)
        if mapping.transform == "year":
            col = F.year(F.to_date(col))
        elif mapping.transform == "month":
            col = F.month(F.to_date(col))
        elif mapping.transform == "day":
            col = F.dayofmonth(F.to_date(col))
        if mapping.concept_map:
            from itertools import chain

            mapping_expr = F.create_map(
                list(
                    chain(*[(F.lit(k), F.lit(v)) for k, v in mapping.concept_map.items()])
                )
            )
            col = mapping_expr.getItem(col)
    elif mapping.default is not None:
        col = F.lit(mapping.default)
    else:
        col = F.lit(None)
    return col


def map_person(df: DataFrame, config: MappingConfig) -> DataFrame:
    """Map flattened FHIR Patient DataFrame into OMOP PERSON format.

    Parameters
    ----------
    df:
        Flattened FHIR Patient DataFrame (output of
        :func:`fhiromop.parser.read_patient_ndjson`).
    config:
        Mapping configuration loaded via :func:`fhiromop.config.load_mapping`.

    Returns
    -------
    DataFrame
        DataFrame containing columns defined in the mapping configuration in the
        order specified.
    """
    cols: List[F.Column] = []
    for target, field_cfg in config.fields.items():
        cols.append(_apply_field(df, field_cfg).alias(target))
    return df.select(*cols)
