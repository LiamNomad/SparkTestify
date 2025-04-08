import logging
import warnings

import pytest
from pyspark.sql import SparkSession

# Configure logging for spark to be less verbose during tests
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("pyspark").setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def spark_session():
    """
    Pytest fixture to provide a SparkSession for testing.

    - Scope: session (one Spark session per test run)
    - Configured to run locally.
    - Automatically tears down after all tests finish.
    """
    print("\n--- Initializing Spark Session for Test Suite ---")

    warnings.filterwarnings("ignore", category=UserWarning, module="py4j")

    spark_session = (
        SparkSession.builder.appName("SparkTestify-Session")
        .master("local[*]")  # Uses all local cores for parallelism
        .config("spark.ui.enabled", "false")  # No need for Spark UI in test
        .config(
            "spark.sql.shuffle.partitions", "2"
        )  # Small shuffle partitions for faster tests
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        # Disable Hive support for simpler local setup if not needed
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )

    # Set log level within Spark context as well
    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    print("\n--- Tearing Down Spark Session ---")
    spark_session.stop()
