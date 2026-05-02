---
title: "API Reference: readers.csv"
description: "Reference for the CSV reader helper used across the sample job."
---

The `pyspark_template.readers.csv` module provides the narrowest public helper in the project: a single function that reads a CSV file with a header into a Spark DataFrame.

## Import Path

```python
from pyspark_template.readers.csv import read_csv
```

## Source File

`pyspark_template/readers/csv.py`

## Signature

```python
def read_csv(spark: SparkSession, path: str) -> DataFrame
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark` | `SparkSession` | — | Active Spark session used to create the DataFrame reader. |
| `path` | `str` | — | Path to the CSV file to load. |

## Return Type

`DataFrame`

The function returns:

```python
spark.read.csv(path=path header=True)
```

That means:

- headers are enabled
- schema inference is not enabled
- no delimiter or quote options are overridden

## Basic Example

```python
from pyspark_template.readers.csv import read_csv
from pyspark_template.utils.spark import get_spark_session

spark = get_spark_session()
drugs_df = read_csv(spark, "data/drugs.csv")

drugs_df.show()
```

Expected schema for the sample file:

```text
root
 |-- atccode: string (nullable = true)
 |-- drug: string (nullable = true)
```

## Combining With Other Helpers

This function becomes more useful when chained into the transform layer:

```python
from pyspark_template.readers.csv import read_csv
from pyspark_template.transform.common import prefix_cols, prepared_title_array

pubmed_df = (
    read_csv(spark, "data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)
```

## Operational Notes

- Because the function does not infer schema, every column is read as a string in the sample datasets.
- Because there is no validation wrapper, bad paths or malformed CSV data surface as Spark-level errors.
- Because the helper is so thin, it is a good place to extend the template if you later need explicit schemas, multiline support, or corrupt-record handling.

## Common Pattern In This Repository

`read_csv` is always the first step in a larger chain rather than the final abstraction. In `pyspark_template/jobs/drugs_gen.py`, the PubMed and clinical-trial inputs are immediately transformed after loading:

```python
prepared_pubmed = (
    read_csv(spark, "data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)
```

That is a useful design clue if you add more readers. Keep the reader responsible for loading bytes into a DataFrame, then move dataset-specific cleanup and enrichment into transform functions that stay independently testable.

## Related APIs

- [`drugs_gen`](/docs/api-reference/jobs-drugs-gen)
- [`prefix_cols`](/docs/api-reference/transform-common)
- [`get_spark_session`](/docs/api-reference/utils-spark)
