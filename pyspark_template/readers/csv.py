from pyspark.sql import DataFrame, SparkSession


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path=path, header=True)
