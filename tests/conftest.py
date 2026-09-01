"""This file configures pytest and provides fixtures for local Spark testing."""

import pathlib
import json
import csv
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # This spins up a tiny, local, offline Spark instance just for tests
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pipeline-tests") \
        .getOrCreate()

@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:
        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """
    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader