"""Configuration helpers for mapping FHIR resources to OMOP tables."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml

__all__ = ["FieldMapping", "MappingConfig", "load_mapping"]


@dataclass
class FieldMapping:
    """Configuration for a single OMOP field."""

    source: str | None = None
    """Name of the source column in the flattened FHIR DataFrame."""

    transform: str | None = None
    """Optional transformation applied to the source column (``year``,
    ``month`` or ``day`` for date columns)."""

    concept_map: Dict[str, int] | None = None
    """Optional mapping from source string values to OMOP concept identifiers."""

    default: Any | None = None
    """Default value used when no source column is provided."""


@dataclass
class MappingConfig:
    """Container for mapping configuration."""

    fields: Dict[str, FieldMapping]


def load_mapping(path: str | Path) -> MappingConfig:
    """Load mapping configuration from a YAML file.

    The YAML file must define a mapping where keys correspond to target OMOP
    fields and values describe how those fields are constructed.  Minimal
    validation is performed to ensure each field mapping contains either a
    ``source`` or ``default`` attribute.
    """
    with open(path, "r", encoding="utf8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("Mapping config must be a mapping")

    fields: Dict[str, FieldMapping] = {}
    for target, cfg in raw.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"Configuration for '{target}' must be a mapping")
        if "source" not in cfg and "default" not in cfg:
            raise ValueError(
                f"Configuration for '{target}' must have 'source' or 'default'"
            )
        fields[target] = FieldMapping(
            source=cfg.get("source"),
            transform=cfg.get("transform"),
            concept_map=cfg.get("concept_map"),
            default=cfg.get("default"),
        )
    return MappingConfig(fields=fields)
