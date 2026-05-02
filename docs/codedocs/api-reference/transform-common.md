---
title: "API Reference: transform.common"
description: "Reference for the reusable DataFrame transforms that normalize column names and title tokens."
---

The `pyspark_template.transform.common` module contains the most reusable logic in the repository. These functions are where the sample job's matching behavior is defined.

## Import Path

```python
from pyspark_template.transform.common import prefix_cols, prepared_title_array
```

## Source File

`pyspark_template/transform/common.py`

## `prefix_cols`

### Signature

```python
def prefix_cols(df: DataFrame, prefix: str) -> DataFrame
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Input DataFrame whose columns should be renamed. |
| `prefix` | `str` | — | Prefix string prepended to every column name. |

### Return Type

`DataFrame`

### Behavior

The function returns:

```python
df.select([f.col(c).alias(f"{prefix}_{c}") for c in df.columns])
```

Every source column is preserved, but each one is renamed to include the prefix.

### Example

```python
prefixed = prefix_cols(df, "pubmed")
prefixed.columns
```

Expected output:

```python
["pubmed_id", "pubmed_title", "pubmed_date", "pubmed_journal"]
```

## `prepared_title_array`

### Signature

```python
def prepared_title_array(df: DataFrame, title: str) -> DataFrame
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Input DataFrame containing the title column. |
| `title` | `str` | — | Name of the title column to normalize and tokenize. |

### Return Type

`DataFrame`

### Behavior

The function appends a new column named `f"{title}_array"` and fills it by:

1. uppercasing the original title
2. splitting on spaces
3. removing commas from each token

### Example

```python
prepared = prepared_title_array(df, "title")
prepared.select("title", "title_array").show(truncate=False)
```

Expected output:

```text
+--------------------------+------------------------------+
|title                     |title_array                   |
+--------------------------+------------------------------+
|hello, world is beautiful |[HELLO, WORLD, IS, BEAUTIFUL] |
+--------------------------+------------------------------+
```

## Combined Pattern

This is how the job uses both functions together:

```python
prepared_pubmed = (
    read_csv(spark, "data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)
```

That pattern is a good default for any future source dataset whose raw column names would otherwise collide during joins.

## Test Coverage

The expected behavior is asserted directly in `tests/transform/test_common.py`:

- `test_prefix_cols` verifies that column names are renamed exactly.
- `test_prepared_title_array` verifies uppercase conversion, splitting, and comma removal.

## Related APIs

- [`read_csv`](/docs/api-reference/readers-csv)
- [`drugs_gen`](/docs/api-reference/jobs-drugs-gen)
- [`write_json`](/docs/api-reference/writers-json)
