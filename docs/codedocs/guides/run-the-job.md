---
title: "Run The Job"
description: "Execute the sample PySpark job end to end and inspect the generated JSON result."
---

This guide walks through the repository's main use case: run the bundled `drugs_gen` CLI against the sample CSV files and verify the single-file JSON output.

## When To Use This Guide

Use this flow when you want to:

- validate that your local Spark and Java setup works
- understand the expected shape of the output file
- reproduce the behavior asserted by `tests/jobs/test_drugs_gen.py`

## End-To-End Flow

<Steps>
<Step>
### Prepare the Python and Spark environment

The README expects Python 3.10, Java 11, Spark 3.5.6, and these environment variables:

```bash
export JAVA_HOME=/path/to/jdk11
export SPARK_HOME=/path/to/spark-3.5.6-bin-hadoop3
export PYSPARK_PYTHON=python3.10
export PYSPARK_DRIVER_PYTHON=python3.10
```

Install dependencies with Poetry:

```bash
pip install poetry
poetry install
```

</Step>
<Step>
### Run the packaged CLI entry point

The CLI is declared in `pyproject.toml` as `drugs_gen = "pyspark_template.jobs.drugs_gen:drugs_gen"`. Run it with explicit file paths so the invocation is self-documenting:

```bash
poetry run drugs_gen \
  --drugs data/drugs.csv \
  --pubmed data/pubmed.csv \
  --clinicals_trials data/clinical_trials.csv \
  --output data/output/result.json
```

You should see:

```text
Writing drugs result in file data/output/result.json
```

</Step>
<Step>
### Inspect the result

Open the generated file:

```bash
sed -n '1,80p' data/output/result.json
```

You should find one object per drug from `data/drugs.csv`, with nested arrays for `pubmeds`, `clinical_trials`, and `journals`.

```json
{
  "atccode": "A04AD",
  "drug": "DIPHENHYDRAMINE",
  "pubmeds": [
    {
      "pubmed_id": "3",
      "pubmed_title": "Diphenhydramine hydrochloride helps symptoms of ciguatera fish poisoning.",
      "pubmed_date": "02/01/2019",
      "pubmed_journal": "The Journal of pediatrics"
    }
  ]
}
```

</Step>
</Steps>

## Why This Works

The CLI in `pyspark_template/jobs/drugs_gen.py` composes the full pipeline:

- `get_spark_session()` creates the session.
- `read_csv()` loads each CSV file with headers.
- `prefix_cols()` and `prepared_title_array()` normalize the PubMed and clinical-trial datasets.
- Spark joins and aggregates assemble the nested arrays.
- `write_json()` converts the result into a single JSON document.

## Common Variations

If you want to write somewhere else:

```bash
poetry run drugs_gen -o /tmp/pyspark-template-result.json
```

If you want to swap in your own input data, preserve the expected schemas:

- `drugs.csv`: `atccode`, `drug`
- `pubmed.csv`: `id`, `title`, `date`, `journal`
- `clinical_trials.csv`: `id`, `scientific_title`, `date`, `journal`

## Troubleshooting

<Callout type="warn">The CLI does not validate schemas before joining. If your replacement CSV files are missing columns such as `title` or `scientific_title`, the failure will happen inside Spark transformations rather than as a clean command-line validation error.</Callout>

Useful checks:

- If Spark fails to start, inspect `JAVA_HOME` and `SPARK_HOME` first.
- If the command succeeds but the output contains arrays like `[{}]`, that is current template behavior for unmatched rows rather than a file corruption issue.
- If you want the fastest smoke test, `make run` wraps linting, tests, and CLI execution in one target from the root `Makefile`.
