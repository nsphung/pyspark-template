---
title: "API Reference: jobs.drugs_gen"
description: "Reference for the CLI entry point that composes the entire pipeline."
---

The `pyspark_template.jobs.drugs_gen` module is the composition root of the repository. It exposes one supported entry point, `drugs_gen`, which is wired to Poetry's script system and acts as the CLI command users run in local development.

## Import Path

```python
from pyspark_template.jobs.drugs_gen import drugs_gen
```

## Source File

`pyspark_template/jobs/drugs_gen.py`

## Signature

```python
def drugs_gen(
    drugs: str,
    pubmed: str,
    clinicals_trials: str,
    output: str,
) -> None
```

Although Click decorates the function, the underlying Python signature stays the same.

## CLI Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `drugs` | `str` | `data/drugs.csv` | Path to the drug catalog CSV. |
| `pubmed` | `str` | `data/pubmed.csv` | Path to the PubMed article CSV. |
| `clinicals_trials` | `str` | `data/clinical_trials.csv` | Path to the clinical trials CSV. |
| `output` | `str` | `data/output/result.json` | Final single JSON file to write. |

The Click flags are:

```text
--drugs, -d
--pubmed, -p
--clinicals_trials, -c
--output, -o
```

## Behavior

`drugs_gen` performs the following steps:

1. Acquire a Spark session with `get_spark_session()`.
2. Read the three CSV inputs using `read_csv()`.
3. Prefix and tokenize the PubMed and clinical-trial title columns with `prefix_cols()` and `prepared_title_array()`.
4. Join the drug list against both prepared datasets.
5. Aggregate nested arrays of source records and journal pairs.
6. Write the final JSON file with `write_json()`.

The join logic is exact-token based:

```python
f.array_contains(f.col("pubmed_title_array"), f.upper(f.col("drug")))
```

and

```python
f.array_contains(
    f.col("clinical_trials_scientific_title_array"),
    f.upper(f.col("drug")),
)
```

## Usage Example

```bash
poetry run drugs_gen \
  --drugs data/drugs.csv \
  --pubmed data/pubmed.csv \
  --clinicals_trials data/clinical_trials.csv \
  --output data/output/result.json
```

Expected command output:

```text
Writing drugs result in file data/output/result.json
```

## Programmatic Example

You normally invoke this through Click, but the module composition is still useful to understand if you want to build a second job:

```python
from pyspark_template.jobs.drugs_gen import drugs_gen

# Click manages option parsing when invoked from the command line.
# For custom jobs, reuse the helpers it calls rather than importing
# and calling the decorated function directly.
```

## Return Type

`None`. The function performs side effects only:

- prints the output location
- writes a JSON file to disk
- triggers Spark actions such as `show()` and writer execution

## Related APIs

- [`read_csv`](/docs/api-reference/readers-csv)
- [`prefix_cols` and `prepared_title_array`](/docs/api-reference/transform-common)
- [`get_spark_session`](/docs/api-reference/utils-spark)
- [`write_json`](/docs/api-reference/writers-json)

## Notes

The repository also contains `pyspark_template/__main__.py`, but that module only prints `"Unuse DE Main Routine."` and is not the supported execution path. The actual user-facing entry point is the Click command documented here.
