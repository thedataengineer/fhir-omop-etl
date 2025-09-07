import json

import pandas as pd

from fhiromop.config import load_mapping
from fhiromop.loader import save_person
from fhiromop.mapper import map_person
from fhiromop.parser import read_patient_ndjson
from fhiromop.qc import run_checks


def test_full_etl_pipeline(tmp_path, spark):
    # Prepare NDJSON input
    patient_path = tmp_path / "patients.ndjson"
    patient_record = {
        "id": "1",
        "gender": "female",
        "birthDate": "1980-01-02",
    }
    patient_path.write_text(json.dumps(patient_record))

    # Load mapping configuration
    mapping = load_mapping("mapping/fhir_to_omop_person.yaml")

    # Parse and map
    flat = read_patient_ndjson(spark, str(patient_path))
    person = map_person(flat, mapping)

    # Save output
    out_csv = tmp_path / "person.csv"
    save_person(person, str(out_csv))

    # Quality checks against saved file
    checks = run_checks(person, str(out_csv))
    assert checks["row_count_match"]
    assert checks["missing"] == {c: 0 for c in person.columns}

    # Verify file content
    loaded = pd.read_csv(out_csv)
    assert len(loaded) == 1
    assert loaded.loc[0, "gender_concept_id"] == 8532
