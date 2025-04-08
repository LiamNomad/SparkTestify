from typing import Optional

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (BooleanType, IntegerType, StringType,
                               StructField, StructType)

from sparktest import assert_spark_function, assert_udf


class TestUdfAssertions:
    # --- Sample Python functions to be used as UDFs ---
    @staticmethod
    def simple_adder(x: int, y: int) -> int:
        if x is None or y is None:
            return None
        return x + y

    @staticmethod
    def string_upper(s: str) -> Optional[str]:
        return s.upper() if s is not None else None

    @staticmethod
    def check_positive(val: int) -> bool:
        # Returns boolean, handles null input implicitly via type hint if strict? No, check needed.
        if val is None:
            return False  # Or None, depending on desired behavior
        return val > 0

    # --- Tests for assert_udf ---

    def test_assert_udf_simple_adder(self, spark_session: SparkSession):
        input_data = [(1, 2), (10, -5), (None, 5), (3, None)]
        expected_output = [3, 5, None, None]
        assert_udf(
            spark_session,
            self.simple_adder,
            input_data=input_data,
            expected_output=expected_output,
            input_schema=["col_a", "col_b"],  # Use list of names
            expected_return_type=IntegerType(),
        )

    def test_assert_udf_string_upper(self, spark_session: SparkSession):
        input_data = ["hello", "World", None, ""]
        expected_output = ["HELLO", "WORLD", None, ""]
        assert_udf(
            spark_session,
            self.string_upper,
            input_data=input_data,  # Single input column
            expected_output=expected_output,
            # Let sparkTestify infer input column name ('input' by default if single col)
            # Alternatively provide: input_schema=["text_in"]
            expected_return_type=StringType(),
        )

    def test_assert_udf_boolean_return(self, spark_session: SparkSession):
        input_data = [10, -5, 0, None]
        expected_output = [True, False, False, False]  # Based on check_positive logic
        assert_udf(
            spark_session,
            self.check_positive,
            input_data=input_data,
            expected_output=expected_output,
            input_schema=["value"],
            expected_return_type=BooleanType(),
        )

    def test_assert_udf_output_mismatch_fails(self, spark_session: SparkSession):
        input_data = ["a", "b"]
        expected_output = ["A", "C"]  # Mismatch
        with pytest.raises(AssertionError, match="UDF 'test_udf' output mismatch"):
            assert_udf(
                spark_session,
                self.string_upper,
                input_data,
                expected_output,
                expected_return_type=StringType(),
            )

    def test_assert_udf_missing_return_type_fails(self, spark_session: SparkSession):
        input_data = ["a"]
        expected_output = ["A"]
        with pytest.raises(TypeError, match="expected_return_type .* is required"):
            assert_udf(
                spark_session, self.string_upper, input_data, expected_output
            )  # No return type

    def test_assert_udf_different_lengths_fails(self, spark_session: SparkSession):
        input_data = [1, 2]
        expected_output = [True]  # Length mismatch
        with pytest.raises(ValueError, match="must have the same length"):
            assert_udf(
                spark_session,
                self.check_positive,
                input_data,
                expected_output,
                expected_return_type=BooleanType(),
            )

    def test_assert_udf_empty_input(self, spark_session: SparkSession):
        # Should pass if expected output is also empty
        assert_udf(
            spark_session, self.string_upper, [], [], expected_return_type=StringType()
        )


class TestSparkFunctionAssertions:
    # --- Tests for assert_spark_function ---

    def test_spark_func_upper_col(self, spark_session: SparkSession):
        input_data = [("Alice",), ("bob",), (None,), ("",)]
        expected_output = ["ALICE", "BOB", None, ""]
        assert_spark_function(
            spark_session,
            func.upper(func.col("name")),  # Use Column expression
            input_data=input_data,
            expected_output=expected_output,
            input_schema=["name"],
        )

    def test_spark_func_concat_ws_str_expr(self, spark_session: SparkSession):
        input_data = [("Mr", "Smith"), ("Ms", "Jones"), (None, "Who"), ("Dr", None)]
        expected_output = [
            "Mr Smith",
            "Ms Jones",
            None,
            None,
        ]

        # concat_ws skips nulls in args, but null sep makes result null? No Sep isn't null.
        # Check Spark docs. -> If name is null, result is null? -> Yes.
        # Let's adjust expectation: If any *non-separator* arg is null, Spark < 3.3 returns null. >=3.3 skips nulls.
        # Assuming Spark 3.3+ behavior for test. If testing older Spark, adjust expectation.
        spark_version = tuple(map(int, spark_session.version.split(".")))
        if spark_version >= (3, 3):
            expected_output = ["Mr Smith", "Ms Jones", "Who", "Dr"]  # Nulls ignored
        else:
            expected_output = ["Mr Smith", "Ms Jones", None, None]  # Nulls propagate

        assert_spark_function(
            spark_session,
            "concat_ws(' ', title, name)",  # Use string expression
            input_data=input_data,
            expected_output=expected_output,
            input_schema=["title", "name"],
        )

    def test_spark_func_arithmetic(self, spark_session: SparkSession):
        input_data = [(1, 10), (5, -2), (None, 3), (8, None)]
        # Expect integers if inputs are int, careful with types
        expected_output = [11, 3, None, None]
        assert_spark_function(
            spark_session,
            func.col("a") + func.col("b"),
            input_data=input_data,
            expected_output=expected_output,
            input_schema=StructType(
                [StructField("a", IntegerType()), StructField("b", IntegerType())]
            ),
        )

    def test_spark_func_output_mismatch_fails(self, spark_session: SparkSession):
        input_data = [("a",), ("b",)]
        expected_output = ["A", "C"]  # Mismatch
        with pytest.raises(
            AssertionError, match="Spark function/expression output mismatch"
        ):
            assert_spark_function(
                spark_session,
                func.upper(func.col("name")),
                input_data,
                expected_output,
                ["name"],
            )

    def test_spark_func_invalid_expr_fails(self, spark_session: SparkSession):
        input_data = [(1,)]
        expected_output = [1]
        with pytest.raises(
            AssertionError,
            match="Error applying Spark function/expression.*AnalysisException",
        ):  # Check for AnalysisException mention
            assert_spark_function(
                spark_session,
                "non_existent_col + 1",
                input_data,
                expected_output,
                ["val"],
            )

    def test_spark_func_empty_input(self, spark_session: SparkSession):
        assert_spark_function(spark_session, func.upper(func.col("a")), [], [], ["a"])

    def test_spark_func_single_input_infer_name(self, spark_session: SparkSession):
        # Test if default 'input' column name works when schema isn't given
        input_data = ["a", "b"]
        expected_output = ["A", "B"]
        assert_spark_function(
            spark_session,
            func.upper(func.col("input")),  # Reference the inferred default name
            input_data=input_data,
            expected_output=expected_output
            # No input_schema provided
        )
