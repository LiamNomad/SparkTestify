from sparktest.assertions import assert_dataframe_equal, assert_schema_equal
from sparktest.mocks import create_mock_dataframe


def test_dataframe_equality(spark):
    data = [(1, "Alice"), (2, "Bob")]
    schema = ["id", "name"]

    df1 = create_mock_dataframe(spark, data, schema)
    df2 = create_mock_dataframe(spark, data, schema)

    assert_dataframe_equal(df1, df2)


def test_dataframe_inequality(spark):
    data1 = [(1, "Alice"), (2, "Bob")]
    data2 = [(1, "Alice"), (3, "Charlie")]
    schema = ["id", "name"]

    df1 = create_mock_dataframe(spark, data1, schema)
    df2 = create_mock_dataframe(spark, data2, schema)

    try:
        assert_dataframe_equal(df1, df2)
    except AssertionError:
        assert True
    else:
        assert False


def test_schema_equality(spark):
    data = [(1, "Alice"), (2, "Bob")]
    schema = ["id", "name"]

    df = create_mock_dataframe(spark, data, schema)

    expected_schema = "struct<id:bigint,name:string>"

    # Convert integer to long (bigint) in Spark
    df = df.withColumn("id", df["id"].cast("bigint"))

    assert_schema_equal(df, expected_schema)
