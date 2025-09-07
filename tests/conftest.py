import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure package root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("fhiromop-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()
