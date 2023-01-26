from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from pyspark_template.transform.common import prefix_cols, prepared_title_array


def test_prefix_cols(spark: SparkSession) -> None:

    # Given
    df: DataFrame = spark.createDataFrame(
        [
            (
                1,
                "hello",
            ),
            (
                2,
                "foobar",
            ),
        ],
        StructType(
            [
                StructField("id", LongType(), False),
                StructField("title", StringType(), False),
            ]
        ),
    )

    expected_df: DataFrame = spark.createDataFrame(
        [
            (
                1,
                "hello",
            ),
            (
                2,
                "foobar",
            ),
        ],
        StructType(
            [
                StructField("prefix_id", LongType(), False),
                StructField("prefix_title", StringType(), False),
            ]
        ),
    )

    # When
    actual_df: DataFrame = prefix_cols(df, "prefix")

    # Then
    assert_df_equality(actual_df, expected_df)


def test_prepared_title_array(spark: SparkSession) -> None:

    # Given
    df: DataFrame = spark.createDataFrame(
        [
            (
                1,
                "hello, world is beautiful",
            ),
            (
                2,
                "foobar is a kind of fish",
            ),
        ],
        StructType(
            [
                StructField("id", LongType(), False),
                StructField("title", StringType(), False),
            ]
        ),
    )

    expected_df: DataFrame = spark.createDataFrame(
        [
            (1, "hello, world is beautiful", ["HELLO", "WORLD", "IS", "BEAUTIFUL"]),
            (
                2,
                "foobar is a kind of fish",
                ["FOOBAR", "IS", "A", "KIND", "OF", "FISH"],
            ),
        ],
        StructType(
            [
                StructField("id", LongType(), False),
                StructField("title", StringType(), False),
                StructField("title_array", ArrayType(StringType(), False), False),
            ]
        ),
    )

    # When
    actual_df: DataFrame = prepared_title_array(df, "title")

    # Then
    assert_df_equality(actual_df, expected_df)
