# Comparing FHIR-OMOP ETL Approaches

This document contrasts this project's lightweight, educational ETL pipeline with other publicly
available efforts for converting FHIR data to the Observational Medical Outcomes Partnership
(OMOP) Common Data Model (CDM).

## Project Goals

The goal of this repository is to provide a concise, PySpark-based example that demonstrates how
Synthea-generated FHIR resources can be transformed into OMOP v5.4-compliant tables.  The focus is
on clarity and extensibility for newcomers rather than full production readiness.

## Other Open-Source Options

### OHDSI FHIR-to-OMOP ETL
* **Tech stack:** Java, Spark, and the OHDSI tool chain
* **Scope:** Comprehensive extraction of multiple FHIR resources into a complete OMOP CDM
* **Strengths:** Mature, community supported, covers broad FHIR resource set, includes concept
  vocabulary loading
* **Trade-offs:** Requires heavier infrastructure and deeper knowledge of OHDSI tooling; mapping
  rules are spread across multiple configuration files and SQL scripts

### Synthea Built-In OMOP Export
* **Tech stack:** Java; runs as part of Synthea generation
* **Scope:** Converts Synthea's simulated FHIR output directly into OMOP CSVs
* **Strengths:** Maintained by Synthea project, produces full OMOP datasets out of the box
* **Trade-offs:** Less flexible for custom mappings or extension beyond Synthea's data model; not
  intended as a reusable ETL for arbitrary FHIR sources

### fhir2omop Community Projects
* **Examples:** `fhir2omop` (Python), `omop-fhir-etl` (Scala)
* **Scope:** Community-driven efforts with varying levels of completeness
* **Strengths:** Diverse language support and experimentation with mapping strategies
* **Trade-offs:** Often target specific use cases, may lack documentation, tests, or ongoing
  maintenance

## How This Repository Differs

* **Educational focus:** Simplified mappings and in-memory concept dictionaries are used to keep
  the code approachable for new contributors.
* **Config-driven mappings:** YAML files describe column mappings, making it easy to extend or
  override rules for additional resources.
* **Test coverage:** Pytest-based unit tests and a miniature integration test validate parser,
  mapper, loader, configuration, and quality-check components.
* **Lightweight dependencies:** PySpark and PyYAML are the main external requirements; the project
  avoids complex deployment assumptions.

## When to Use What

| Use Case | Recommended Approach |
| --- | --- |
| Learning how FHIR can map to OMOP | **This repo** offers a small, readable pipeline with sample data |
| Generating large OMOP datasets from Synthea | **Synthea's OMOP export** for turnkey conversion |
| Production ETL for clinical FHIR servers | **OHDSI FHIR-to-OMOP ETL** for robustness and community support |
| Experimenting with alternative languages or mappings | **Community `fhir2omop` projects** or forks of this repo |

## Further Reading

* [OHDSI FHIR workgroup](https://www.ohdsi.org) for updates on FHIR-to-OMOP initiatives
* [Synthea Documentation](https://synthetichealth.github.io/synthea/) for details on its OMOP
  exports
* [OMOP CDM](https://ohdsi.github.io/CommonDataModel/) for specifications of the target schema

