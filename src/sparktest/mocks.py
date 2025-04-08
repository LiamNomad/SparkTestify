from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession


def create_mock_dataframe(
    spark: SparkSession, data: List[Tuple], schema: List[str]
) -> DataFrame:
    """
    Create a mock DataFrame from data and schema.

    Args:
        spark (SparkSession): Active SparkSession.
        data (List[Tuple]): Data rows.
        schema (List[str]): Column names.

    Returns:
        DataFrame: Spark DataFrame created from mock data.
    """
    return spark.createDataFrame(data, schema=schema)
