from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from sparktest.assertions import assert_dataframe_equal, assert_schema_equal


def read_data(spark):
    return spark.createDataFrame([("alice", 25), ("bob", 30)], ["name", "age"])


def transform_data(df):
    return df.withColumn("age_plus_ten", col("age") + 10)


def run_pipeline(spark):
    df_raw = read_data(spark)
    df_transformed = transform_data(df_raw)
    return df_transformed


def expected_output(spark):
    return spark.createDataFrame(
        [("alice", 25, 35), ("bob", 30, 40)], ["name", "age", "age_plus_ten"]
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]").appName("PipelineDemo").getOrCreate()
    )

    actual_df = run_pipeline(spark)
    expected_df = expected_output(spark)

    assert_schema_equal(actual_df.schema, expected_df.schema)
    assert_dataframe_equal(actual_df, expected_df)

    print("Pipeline test passed ✅")
    spark.stop()
