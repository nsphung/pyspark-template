"""drugs generation."""

import click
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

from pyspark_template.readers.csv import read_csv
from pyspark_template.transform.common import prefix_cols, prepared_title_array
from pyspark_template.utils.spark import get_spark_session
from pyspark_template.writers.json import write_json


@click.command()
@click.option(
    "--drugs", "-d", default="data/drugs.csv", type=str, help="Path to drugs.csv"
)
@click.option(
    "--pubmed", "-p", default="data/pubmed.csv", type=str, help="Path to pubmed.csv"
)
@click.option(
    "--clinicals_trials",
    "-c",
    default="data/clinical_trials.csv",
    type=str,
    help="Path to clinical_trials.csv",
)
@click.option(
    "--output",
    "-o",
    default="data/output/result.json",
    type=str,
    help="Output path to result.json (e.g /path/to/result.json)",
)
def drugs_gen(drugs: str, pubmed: str, clinicals_trials: str, output: str) -> None:
    spark: SparkSession = get_spark_session()

    # Read data
    drugs_df: DataFrame = read_csv(spark, drugs)
    prepared_pubmed: DataFrame = (
        read_csv(spark, pubmed)
        .transform(lambda df: prefix_cols(df, "pubmed"))
        .transform(lambda df: prepared_title_array(df, "pubmed_title"))
    )
    prepared_clinical_trials: DataFrame = (
        read_csv(spark, clinicals_trials)
        .transform(lambda df: prefix_cols(df, "clinical_trials"))
        .transform(
            lambda df: prepared_title_array(df, "clinical_trials_scientific_title")
        )
    )

    # Transform data
    journals: DataFrame = (
        drugs_df.join(
            prepared_pubmed,
            f.array_contains(f.col("pubmed_title_array"), f.upper(f.col("drug"))),
            "outer",
        )
        .join(
            prepared_clinical_trials,
            f.array_contains(
                f.col("clinical_trials_scientific_title_array"), f.upper(f.col("drug"))
            ),
            "left",
        )
        .groupby("atccode", "drug")
        .agg(
            f.collect_set(
                f.struct(
                    f.col("pubmed_id"),
                    f.col("pubmed_title"),
                    f.col("pubmed_date"),
                    f.col("pubmed_journal"),
                )
            ).alias("pubmeds"),
            f.collect_set(
                f.struct(
                    f.col("clinical_trials_id"),
                    f.col("clinical_trials_scientific_title"),
                    f.col("clinical_trials_date"),
                    f.col("clinical_trials_journal"),
                )
            ).alias("clinical_trials"),
            f.collect_set(
                f.struct(
                    f.col("pubmed_date").alias("date"),
                    f.col("pubmed_journal").alias("journal"),
                )
            ).alias("journals_pubmed"),
            f.collect_set(
                f.struct(
                    f.col("clinical_trials_date").alias("date"),
                    f.col("clinical_trials_journal").alias("journal"),
                )
            ).alias("journals_clinical_trials"),
        )
        .withColumn(
            "journals", f.array_union("journals_pubmed", "journals_clinical_trials")
        )
        .drop("journals_pubmed", "journals_clinical_trials")
    )

    journals.show()

    # Write result data
    print(f"Writing drugs result in file {output}")
    write_json(journals, "journals.json", output)
