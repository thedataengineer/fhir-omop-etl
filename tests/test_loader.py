import pandas as pd

from fhiromop.loader import save_person


def test_save_person_writes_csv(tmp_path, spark):
    df = spark.createDataFrame(
        [("1", 8532, 1980)],
        "person_id string, gender_concept_id int, year_of_birth int",
    )
    path = tmp_path / "person.csv"
    save_person(df, str(path))
    assert path.exists()
    loaded = pd.read_csv(path)
    assert list(loaded.columns) == ["person_id", "gender_concept_id", "year_of_birth"]
    assert str(loaded.loc[0, "person_id"]) == "1"
    assert loaded.loc[0, "gender_concept_id"] == 8532
    assert loaded.loc[0, "year_of_birth"] == 1980


def test_save_person_empty_dataframe(tmp_path, spark):
    df = spark.createDataFrame([], "person_id string, gender_concept_id int")
    path = tmp_path / "empty.csv"
    save_person(df, str(path))
    loaded = pd.read_csv(path)
    assert loaded.empty
    assert list(loaded.columns) == ["person_id", "gender_concept_id"]
