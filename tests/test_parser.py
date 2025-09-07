import json

from fhiromop.parser import flatten, read_patient_ndjson


def test_flatten_handles_structs_and_arrays(spark):
    data = [
        {
            "id": "1",
            "name": [{"given": ["Alice", "B"], "family": "Smith"}],
            "address": {"line": ["123 St", "Apt 4"], "city": "Town"},
        }
    ]
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data[0])]))
    flat = flatten(df)
    row = flat.collect()[0]
    assert row.id == "1"
    assert row.name_given == "Alice,B"
    assert row.name_family == "Smith"
    assert row.address_line == "123 St,Apt 4"
    assert row.address_city == "Town"


def test_flatten_array_of_structs_creates_multiple_rows(spark):
    data = {
        "id": "1",
        "name": [
            {"given": ["Alice"], "family": "Smith"},
            {"given": ["Ally"], "family": "Jones"},
        ],
    }
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
    flat = flatten(df)
    assert flat.count() == 2
    families = {r.name_family for r in flat.collect()}
    assert families == {"Smith", "Jones"}


def test_read_patient_ndjson(tmp_path, spark):
    path = tmp_path / "patients.ndjson"
    content = """{"id": "1", "gender": "female"}
{"id": "2", "gender": "male"}
"""
    path.write_text(content)
    df = read_patient_ndjson(spark, str(path))
    assert set(df.columns) == {"id", "gender"}
    assert df.count() == 2


def test_read_patient_ndjson_empty(tmp_path, spark):
    path = tmp_path / "empty.ndjson"
    path.write_text("")
    df = read_patient_ndjson(spark, str(path))
    assert df.count() == 0
    assert df.columns == []
