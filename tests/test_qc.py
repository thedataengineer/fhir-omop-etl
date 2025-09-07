import pandas as pd

from fhiromop.qc import missing_counts, row_count_matches, run_checks


def test_row_count_matches(tmp_path, spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    ref = tmp_path / "ref.csv"
    pd.DataFrame({"id": [1, 2]}).to_csv(ref, index=False)
    assert row_count_matches(df, str(ref))
    pd.DataFrame({"id": [1]}).to_csv(ref, index=False)
    assert not row_count_matches(df, str(ref))


def test_row_count_matches_missing_file(tmp_path, spark):
    df = spark.createDataFrame([], "id int")
    missing_path = tmp_path / "missing.csv"
    assert row_count_matches(df, str(missing_path))
    df_non_empty = spark.createDataFrame([(1,)], ["id"])
    assert not row_count_matches(df_non_empty, str(missing_path))


def test_missing_counts(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], ["a", "b"])
    counts = missing_counts(df)
    assert counts["a"] == 0
    assert counts["b"] == 1


def test_run_checks(tmp_path, spark):
    df = spark.createDataFrame([(1, None)], "a int, b string")
    ref = tmp_path / "ref.csv"
    pd.DataFrame({"a": [1]}).to_csv(ref, index=False)
    result = run_checks(df, str(ref))
    assert result["row_count_match"]
    assert result["missing"]["b"] == 1
