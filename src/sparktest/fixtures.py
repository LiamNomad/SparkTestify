# src/sparktest/fixtures.py

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture to provide a SparkSession for testing.

    - Scope: session (one Spark session per test run)
    - Configured to run locally.
    - Automatically tears down after all tests finish.
    """
    spark_session = (
        SparkSession.builder
        .appName("SparkTestifyTestSession")
        .master("local[*]")  # Uses all local cores for parallelism
        .config("spark.ui.enabled", "false")  # No need for Spark UI in test
        .config("spark.sql.shuffle.partitions", "2")  # Small shuffle partitions for faster tests
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
