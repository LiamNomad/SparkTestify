import warnings
from typing import Any, Callable, List, Optional, Tuple, Union

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DataType, StructField, StructType

from sparktest.assertions import assert_dataframe_equal


def assert_udf(
    spark: SparkSession,
    udf_func: Callable,
    input_data: List[Union[Tuple, Row, Any]],
    expected_output: List[Any],
    input_schema: Optional[Union[StructType, List[str]]] = None,
    expected_return_type: Optional[DataType] = None,
    udf_name: str = "test_udf",
    output_col_name: str = "result",
    input_col_names: Optional[List[str]] = None,
) -> None:
    """
    Asserts the output of a Python UDF against expected values.

    Creates a temporary DataFrame, registers the UDF (if not already),
    applies it, and compares the result column.

    Args:
        spark: The SparkSession instance.
        udf_func: The Python function to be tested as a UDF.
        input_data: A list of input values or tuples/Rows for the UDF.
                    If tuples/Rows, input_col_names must match the structure.
                    If single values, input_col_names should be a single-element list.
        expected_output: A list of expected output values from the UDF,
                         corresponding element-wise to input_data.
        input_schema: Optional schema or list of column names for the input DataFrame.
                      If None, schema is inferred (requires input_data to be Rows/tuples).
                      If a list of names, types are inferred by Spark.
        expected_return_type: The expected Spark SQL DataType the UDF should return.
                              Required for registering the UDF.
        udf_name: The name to register the UDF with. Defaults to "test_udf".
        output_col_name: The name for the column containing the UDF result.
        input_col_names: List of column names from the temporary input DataFrame
                         to pass to the UDF. If None, assumes a single input column
                         named "input" or uses names from input_schema if provided.

    Raises:
        AssertionError: If UDF output doesn't match expected_output or other setup issues.
        ValueError: If arguments are inconsistent (e.g., mismatch in list lengths).
        TypeError: If expected_return_type is missing or not a DataType.
    """
    if len(input_data) != len(expected_output):
        raise ValueError(
            "Input data list and expected output list must have the same length."
        )
    if not expected_return_type:
        raise TypeError(
            "expected_return_type (Spark DataType) is required for registering the UDF."
        )
    if not isinstance(expected_return_type, DataType):
        raise TypeError(
            f"expected_return_type must be a Spark DataType, got {type(expected_return_type)}"
        )

    # --- Prepare Input DataFrame ---
    if not input_data:  # Handle empty input case
        warnings.warn("Input data is empty, UDF assertion might not be meaningful.")
        # Create empty DF with expected output schema to satisfy comparison structure
        expected_df = spark.createDataFrame(
            [], StructType([StructField(output_col_name, expected_return_type, True)])
        )
        # Check if UDF raises error on empty DF (optional, depends on UDF logic)
        # For now, we just assert based on the (empty) expected output.
        actual_output_collected = []
    else:
        # Wrap single values in tuples for createDataFrame
        processed_input = [
            (i,) if not isinstance(i, (tuple, Row)) else i for i in input_data
        ]

        if input_schema:
            input_df = spark.createDataFrame(processed_input, schema=input_schema)
        else:
            try:
                input_df = spark.createDataFrame(processed_input)  # Infer schema
            except Exception as e:
                raise ValueError(
                    f"Could not infer schema from input_data. Provide input_schema. Spark error: {e}"
                )

        # Determine input columns for UDF call
        if input_col_names is None:
            if len(input_df.columns) == 1:
                input_col_names = input_df.columns
            else:
                # Cannot reliably guess multi-column input without names or schema
                raise ValueError(
                    "input_col_names must be specified for multi-column input when input_schema isn't fully defined."
                )
        elif not all(col in input_df.columns for col in input_col_names):
            raise ValueError(
                f"One or more input_col_names ({input_col_names}) not found in "
                f"inferred/provided input DataFrame columns: {input_df.columns}"
            )

        input_cols = [F.col(c) for c in input_col_names]

        # --- Register and Apply UDF ---
        spark.udf.register(udf_name, udf_func, expected_return_type)
        actual_df = input_df.withColumn(
            output_col_name, F.call_udf(udf_name, *input_cols)
        )

        # --- Collect Actual Output ---
        actual_output_collected = [
            row[output_col_name] for row in actual_df.select(output_col_name).collect()
        ]

    # --- Compare Actual vs Expected ---

    # Create expected DataFrame for comparison using assert_dataframe_equal
    # This handles type nuances better than direct list comparison sometimes
    expected_data_rows = [Row(**{output_col_name: val}) for val in expected_output]
    expected_df_schema = StructType(
        [StructField(output_col_name, expected_return_type, True)]
    )  # Assume nullable for comparison flexibility
    expected_df = spark.createDataFrame(expected_data_rows, schema=expected_df_schema)

    # Create actual result DataFrame for comparison
    actual_data_rows = [
        Row(**{output_col_name: val}) for val in actual_output_collected
    ]
    actual_result_df = spark.createDataFrame(
        actual_data_rows, schema=expected_df_schema
    )  # Use same expected schema

    # Use assert_dataframe_ for robust comparison
    try:
        assert_dataframe_equal(expected_df, actual_result_df)
    except AssertionError as e:
        raise AssertionError(
            f"UDF '{udf_name}' output mismatch.\n"
            f"Input Data:\n{input_data}\n"
            f"Expected Output:\n{expected_output}\n"
            f"Actual Output:\n{actual_output_collected}\n"
            f"Comparison Error: {e}"
        )


def assert_spark_function(
    spark: SparkSession,
    func_expr: Union[Column, str],
    input_data: List[Union[Tuple, Row, Any]],
    expected_output: List[Any],
    input_schema: Optional[Union[StructType, List[str]]] = None,
    output_col_name: str = "result",
    input_col_names: Optional[List[str]] = None,
) -> None:
    """
    Asserts the output of a built-in Spark SQL function or expression.

    Creates a temporary DataFrame, applies the function/expression,
    and compares the result column.

    Args:
        spark: The SparkSession instance.
        func_expr: The Spark function/expression to test (e.g., F.upper(F.col("name")),
                   "col_a + col_b", F.expr("some_sql_expr(col_a)")).
        input_data: A list of input values or tuples/Rows. Structure must match
                    the columns used in func_expr.
        expected_output: A list of expected output values, corresponding element-wise
                         to input_data.
        input_schema: Optional schema or list of column names for the input DataFrame.
                      If None, schema is inferred. If list of names, types inferred.
                      Must define the columns referenced in func_expr.
        output_col_name: The name for the column containing the function result.
        input_col_names: Optional, primarily for documentation or if input schema
                         is just a list of names. Ensures clarity about which input columns
                         are used.

    Raises:
        AssertionError: If function output doesn't match expected_output.
        ValueError: If arguments are inconsistent (e.g., mismatch in list lengths, missing columns).
        PySpark AnalysisException: If func_expr is invalid or references non-existent columns.
    """
    if len(input_data) != len(expected_output):
        raise ValueError(
            "Input data list and expected output list must have the same length."
        )

    # --- Prepare Input DataFrame ---
    if not input_data:
        warnings.warn(
            "Input data is empty, Spark function assertion might not be meaningful."
        )
        # Cannot easily determine expected output schema without evaluating the function
        # For simplicity, we'll assume the comparison works if both actual/expected are empty
        actual_output_collected = []
        if expected_output:
            raise AssertionError(
                f"Input is empty, but expected output is not: {expected_output}"
            )

    else:
        processed_input = [
            (i,) if not isinstance(i, (tuple, Row)) else i for i in input_data
        ]

        if input_schema:
            input_df = spark.createDataFrame(processed_input, schema=input_schema)
        else:
            try:
                # Try inferring schema
                input_df = spark.createDataFrame(processed_input)
                if input_col_names and len(input_col_names) != len(input_df.columns):
                    # If names provided, rename inferred columns if count matches
                    if len(processed_input[0]) == len(input_col_names):
                        input_df = input_df.toDF(*input_col_names)
                    else:
                        raise ValueError(
                            "input_col_names length doesn't match inferred columns or tuple length."
                        )
                elif (
                    input_col_names is None
                    and len(input_df.columns) == 1
                    and isinstance(func_expr, str)
                    and "input" in func_expr
                ):
                    # Special case: single inferred column, default name is "input" if needed
                    input_df = input_df.toDF("input")

            except Exception as e:
                raise ValueError(
                    f"Could not infer schema from input_data. Provide input_schema. Spark error: {e}"
                )

        # --- Apply Spark Function/Expression ---
        try:
            if isinstance(func_expr, str):
                # Use expr for string-based expressions
                actual_df = input_df.withColumn(output_col_name, F.expr(func_expr))
            elif isinstance(func_expr, Column):
                # Use withColumn for Column objects
                actual_df = input_df.withColumn(output_col_name, func_expr)
            else:
                raise TypeError(
                    "func_expr must be a Spark Column object or a SQL expression string."
                )

            # --- Collect Actual Output ---
            actual_output_collected = [
                row[output_col_name]
                for row in actual_df.select(output_col_name).collect()
            ]

        except Exception as e:
            # Catch potential PySpark errors during analysis/execution
            raise AssertionError(
                f"Error applying Spark function/expression: {func_expr}\n"
                f"Input DataFrame Schema: {input_df.schema.simpleString()}\n"
                f"Input Data Sample: {input_data[:5]}\n"
                f"Spark Error: {e}"
            ) from e

    # --- Compare Actual vs Expected ---
    # We perform a direct list comparison here, as creating an expected DF
    # requires knowing the exact output type of the Spark function, which can be complex.
    # For more type-sensitive checks, the user might need pre-create expected_df
    # and use assert_dataframe_equal directly.

    assert actual_output_collected == expected_output, (
        f"Spark function/expression output mismatch.\n"
        f"Expression: {func_expr}\n"
        f"Input Data:\n{input_data}\n"
        f"Expected Output:\n{expected_output}\n"
        f"Actual Output:\n{actual_output_collected}"
    )
