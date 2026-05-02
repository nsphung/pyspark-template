---
title: "API Reference: writers.json"
description: "Reference for the JSON writer helpers that turn Spark DataFrames into single-file JSON output."
---

The `pyspark_template.writers.json` module packages all output-related behavior for the sample job. Its helpers can be reused from another job if you want the same single-file JSON export pattern.

## Import Path

```python
from pyspark_template.writers.json import (
    write_json,
    write_json_array,
    write_json_array_pd,
    write_single_json,
)
```

## Source File

`pyspark_template/writers/json.py`

## `write_json_array`

### Signature

```python
def write_json_array(df: DataFrame, output_file: str) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | DataFrame to serialize. |
| `output_file` | `str` | — | Temporary output directory used by Spark's text writer. |

### Return Type

`None`

### Example

```python
write_json_array(result_df, "tmp/journals")
```

This creates Spark text output in `tmp/journals`.

## `write_json_array_pd`

### Signature

```python
def write_json_array_pd(df: DataFrame, output_file: str) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | DataFrame to collect into Pandas. |
| `output_file` | `str` | — | Final JSON file path. |

### Return Type

`None`

### Example

```python
write_json_array_pd(result_df.limit(10), "tmp/sample.json")
```

Use this only for small datasets because it materializes all rows on the driver.

## `write_single_json`

### Signature

```python
def write_single_json(json_dir: str, output_json: str) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `json_dir` | `str` | — | Temporary directory containing Spark-generated `.txt` files. |
| `output_json` | `str` | — | Final pretty-printed JSON file path. |

### Return Type

`None`

### Example

```python
write_single_json("tmp/journals", "tmp/result.json")
```

The function scans the directory, loads one `.txt` file as JSON, and writes it to `output_json`.

## `write_json`

### Signature

```python
def write_json(df: DataFrame, path: str, output_file: str) -> None
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Final DataFrame to export. |
| `path` | `str` | — | Temporary directory used during export. |
| `output_file` | `str` | — | Final single JSON file path. |

### Return Type

`None`

### Behavior

`write_json` is the high-level helper used by `drugs_gen`. It:

1. calls `write_json_array(df, path)`
2. calls `write_single_json(path, output_file)`
3. removes the temporary directory with `shutil.rmtree(path)`

### Example

```python
from pyspark_template.writers.json import write_json

write_json(result_df, "journals.json", "data/output/result.json")
```

## Related APIs

- [`drugs_gen`](/docs/api-reference/jobs-drugs-gen)
- [`JSON Output`](/docs/json-output)
- [`get_spark_session`](/docs/api-reference/utils-spark)
