# src/sparktest/assertions.py

from pyspark.sql import DataFrame


def assert_dataframe_equal(df1: DataFrame, df2: DataFrame, ignore_order: bool = True):
    """
    Assert that two DataFrames are equal.

    Args:
        df1 (DataFrame): First DataFrame.
        df2 (DataFrame): Second DataFrame.
        ignore_order (bool): Whether to ignore row order. Default is True.
    """
    if df1.schema != df2.schema:
        raise AssertionError(f"Schemas do not match:\n{df1.schema}\n!=\n{df2.schema}")

    if ignore_order:
        data1 = set(tuple(row) for row in df1.collect())
        data2 = set(tuple(row) for row in df2.collect())
    else:
        data1 = df1.collect()
        data2 = df2.collect()

    if data1 != data2:
        raise AssertionError(f"DataFrames are not equal:\n{data1}\n!=\n{data2}")


def assert_schema_equal(df: DataFrame, expected_schema: str):
    """
    Assert that a DataFrame's schema matches the expected schema.

    Args:
        df (DataFrame): The DataFrame to check.
        expected_schema (str): Expected schema in DDL format.
    """
    actual_schema = df.schema.simpleString()
    if actual_schema != expected_schema:
        raise AssertionError(
            f"Schema does not match:\nExpected: {expected_schema}\nActual:   {actual_schema}"
        )
