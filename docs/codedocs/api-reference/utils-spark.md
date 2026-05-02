---
title: "API Reference: utils.spark"
description: "Reference for Spark configuration constants and session factory helpers."
---

The `pyspark_template.utils.spark` module centralizes Spark defaults and session creation for the rest of the repository.

## Import Path

```python
from pyspark_template.utils.spark import (
    DEFAULT_MAX_RESULT_SIZE,
    DEFAULT_SHUFFLE_PARTITIONS,
    get_spark_session,
    spark_conf_default,
)
```

## Source File

`pyspark_template/utils/spark.py`

## Constants

### `DEFAULT_SHUFFLE_PARTITIONS`

```python
DEFAULT_SHUFFLE_PARTITIONS: str = "24"
```

Default value passed into `spark_conf_default`.

### `DEFAULT_MAX_RESULT_SIZE`

```python
DEFAULT_MAX_RESULT_SIZE: str = "0"
```

Disables the driver result size cap in the generated `SparkConf`.

## `spark_conf_default`

### Signature

```python
def spark_conf_default(
    shuffle_partitions: str = DEFAULT_SHUFFLE_PARTITIONS,
) -> SparkConf
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `shuffle_partitions` | `str` | `DEFAULT_SHUFFLE_PARTITIONS` | Value assigned to `spark.sql.shuffle.partitions`. |

### Return Type

`SparkConf`

### Behavior

The returned config sets:

- `spark.driver.maxResultsSize`
- `spark.sql.shuffle.partitions`
- `spark.serializer`
- `spark.sql.execution.arrow.pyspark.enabled`
- `spark.sql.execution.arrow.pyspark.fallback.enabled`

### Example

```python
conf = spark_conf_default(shuffle_partitions="8")
print(conf.get("spark.sql.shuffle.partitions"))
```

Expected output:

```text
8
```

## `get_spark_session`

### Signature

```python
def get_spark_session(
    spark_conf: SparkConf = spark_conf_default(),
) -> SparkSession
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `spark_conf` | `SparkConf` | `spark_conf_default()` | Spark configuration applied to the session builder. |

### Return Type

`SparkSession`

### Behavior

- Uses `SparkSession.builder.config(conf=spark_conf)`
- Sets `.appName("pyspark_template_app")`
- Calls `.getOrCreate()`
- Caches the result through `@lru_cache(maxsize=None)`

### Example

```python
spark = get_spark_session()
print(spark.sparkContext.appName)
```

Expected output:

```text
pyspark_template_app
```

### Common Combined Pattern

```python
spark = get_spark_session(spark_conf_default(shuffle_partitions="4"))
df = read_csv(spark, "data/drugs.csv")
```

This is the intended extension point for new jobs. Rather than embedding a long `SparkSession.builder` chain inside every module, create the session once through this helper and pass it into reader functions. That keeps the runtime baseline consistent across CLI commands, notebooks, and tests that choose to share the same setup.

## Operational Notes

- The app name is fixed to `pyspark_template_app` in source.
- The helper does not set a local master, warehouse directory, or driver memory; the dedicated pytest fixture in `tests/conftest.py` covers those test-specific needs separately.
- Because the default argument is `spark_conf_default()`, the module constructs a default `SparkConf` at import time and reuses it unless you pass another one explicitly on the first call.

## Related APIs

- [`read_csv`](/docs/api-reference/readers-csv)
- [`drugs_gen`](/docs/api-reference/jobs-drugs-gen)
- [`Spark Session`](/docs/spark-session)
