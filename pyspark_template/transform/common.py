import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def prefix_cols(df: DataFrame, prefix: str) -> DataFrame:
    return df.select([f.col(c).alias(f"{prefix}_{c}") for c in df.columns])


def prepared_title_array(df: DataFrame, title: str) -> DataFrame:
    return df.withColumn(
        f"{title}_array", f.split(f.upper(f.col(title)), " ")
    ).withColumn(
        f"{title}_array",
        f.transform(f.col(f"{title}_array"), lambda x: f.regexp_replace(x, ",", "")),
    )
