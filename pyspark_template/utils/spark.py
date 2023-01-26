"""
Spark Utility module
"""
from functools import lru_cache

from pyspark import SparkConf
from pyspark.sql import SparkSession

DEFAULT_SHUFFLE_PARTITIONS: str = "24"
DEFAULT_MAX_RESULT_SIZE: str = "0"


def spark_conf_default(
    shuffle_partitions: str = DEFAULT_SHUFFLE_PARTITIONS,
) -> SparkConf:
    return (
        SparkConf()
        .set("spark.driver.maxResultsSize", DEFAULT_MAX_RESULT_SIZE)
        .set("spark.sql.shuffle.partitions", shuffle_partitions)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    )


@lru_cache(maxsize=None)
def get_spark_session(spark_conf: SparkConf = spark_conf_default()) -> SparkSession:
    """
    Builds a spark session object
    For more information about configuration properties, see https://spark.apache.org/docs/latest/configuration.html
    :return: a SparkSession
    """
    return (
        SparkSession.builder.config(conf=spark_conf)
        .appName("pyspark_template_app")
        .getOrCreate()
    )
