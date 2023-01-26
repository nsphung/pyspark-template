import os
import tempfile
from functools import lru_cache

import findspark
import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
@lru_cache(maxsize=None)
def spark(request: FixtureRequest) -> SparkSession:
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[*] pyspark-shell"

    findspark.init()

    """
    Builds a spark session object
    For more information about configuration properties, see https://spark.apache.org/docs/latest/configuration.html
    :return: a SparkSession
    """
    spark_session: SparkSession = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.memory", "3G")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config(
            "spark.sql.warehouse.dir",
            f"{tempfile.gettempdir()}/pyspark-template-test/spark-warehouse",
        )
        .appName("pyspark-template-test")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark_session.stop())

    return spark_session
