import json

import pytest
from pyspark.sql.utils import AnalysisException

from fhiromop.config import load_mapping
from fhiromop.mapper import map_person
from fhiromop.parser import flatten


@pytest.fixture(scope="module")
def mapping_path(tmp_path_factory):
    # Use repository mapping file content for tests
    from pathlib import Path
    repo_mapping = Path("mapping/fhir_to_omop_person.yaml").read_text()
    path = tmp_path_factory.mktemp("mapping") / "map.yaml"
    path.write_text(repo_mapping)
    return str(path)


def test_map_person_transforms_and_defaults(spark, mapping_path):
    df = spark.read.json(
        spark.sparkContext.parallelize(
            ['{"id":"1","gender":"female","birthDate":"1980-01-02"}']
        )
    )
    flat = flatten(df)
    config = load_mapping(mapping_path)
    person = map_person(flat, config)
    row = person.collect()[0]
    assert row.person_id == "1"
    assert row.gender_concept_id == 8532
    assert row.year_of_birth == 1980
    assert row.month_of_birth == 1
    assert row.day_of_birth == 2
    assert row.race_concept_id == 0


def test_map_person_concept_map_unknown(spark, mapping_path):
    df = spark.read.json(
        spark.sparkContext.parallelize(
            ['{"id":"1","gender":"invalid","birthDate":"1980-01-02"}']
        )
    )
    flat = flatten(df)
    config = load_mapping(mapping_path)
    person = map_person(flat, config)
    row = person.collect()[0]
    assert row.gender_concept_id is None


def test_map_person_missing_column_raises(spark, mapping_path):
    df = spark.read.json(spark.sparkContext.parallelize(['{"id":"1"}']))
    flat = flatten(df)
    config = load_mapping(mapping_path)
    with pytest.raises(AnalysisException):
        map_person(flat, config).collect()
