# FHIR to OMOP ETL

A minimal example ETL pipeline that converts FHIR resources into the OMOP Common Data Model using **PySpark**. The project demonstrates transforming Synthea FHIR Patient exports into the OMOP PERSON table.

## Installation

1. Create and activate a Python virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Install Apache Spark and ensure the `SPARK_HOME` environment variable and `PATH` are configured according to the [Spark documentation](https://spark.apache.org/docs/latest/index.html).

## Usage

The snippets below assume you are in the repository root and have access to the sample data in `examples/`.

```python
from pyspark.sql import SparkSession
from fhiromop import parser, mapper, config, loader, qc

spark = SparkSession.builder.getOrCreate()

# Parse FHIR Patient NDJSON
patient_df = parser.read_patient_ndjson(spark, "examples/sample_patient.ndjson")

# Map to OMOP PERSON using YAML configuration
mapping = config.load_mapping("mapping/fhir_to_omop_person.yaml")
person_df = mapper.map_person(patient_df, mapping)

# Run quality checks against sample OMOP output
summary = qc.run_checks(person_df, "examples/sample_omop_person.csv")
print(summary)

# Save PERSON records to CSV
loader.save_person(person_df, "person.csv")
```

## Modules

- `fhiromop.parser` – Read and flatten FHIR NDJSON files.
- `fhiromop.mapper` – Apply mapping rules to generate OMOP tables.
- `fhiromop.config` – Data classes and helpers for loading mapping configurations.
- `fhiromop.loader` – Utilities for writing OMOP-formatted data.
- `fhiromop.qc` – Basic data quality checks.

## Examples

The `examples/` directory contains sample files to get started:

- `sample_patient.ndjson` – Small Synthea Patient export.
- `sample_omop_person.csv` – Sample PERSON table for validation.

## Testing

Run the test suite with:

```bash
pytest
```

Tests require dependencies from `requirements.txt` and an operational Spark installation.
