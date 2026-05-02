---
title: "Getting Started"
description: "Build and run the PySpark template job that links drug names to PubMed articles and clinical trials."
---

`/nsphung/pyspark-template` is a Python 3.10 and PySpark 3.5 template that shows how to organize a small batch job around reusable readers, transforms, Spark session helpers, and JSON writers.

## The Problem

- PySpark projects often start as a single notebook or script, then become hard to test and hard to package.
- Spark setup, file I/O, and transformation logic frequently get mixed together, which makes reuse difficult.
- Teams need a repeatable way to run the same logic locally, from a CLI, or through `spark-submit`.
- Output formats for downstream consumers are usually more opinionated than Spark's default partitioned JSON layout.

## The Solution

This template separates the pipeline into clear layers: `read_csv` handles ingestion, `prefix_cols` and `prepared_title_array` normalize data for matching, `get_spark_session` centralizes Spark configuration, and `write_json` collapses the Spark output into a single JSON artifact. The top-level `drugs_gen` CLI wires those pieces together in `pyspark_template/jobs/drugs_gen.py`.

```python
import pyspark.sql.functions as f

from pyspark_template.readers.csv import read_csv
from pyspark_template.transform.common import prefix_cols, prepared_title_array
from pyspark_template.utils.spark import get_spark_session
from pyspark_template.writers.json import write_json

spark = get_spark_session()
drugs_df = read_csv(spark, "data/drugs.csv")
pubmed_df = (
    read_csv(spark, "data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)

result = drugs_df.join(
    pubmed_df,
    f.array_contains(pubmed_df.pubmed_title_array, f.upper(drugs_df.drug)),
    "outer",
)

write_json(result, "journals.json", "data/output/result.json")
```

## Installation

" "bun"]}>
<Tab value="npm">

```bash
# This project is not published to npm.
# Use Poetry or pip in a Python 3.10 environment instead.
git clone https://github.com/nsphung/pyspark-template.git
cd pyspark-template
pip install poetry
poetry install
```

</Tab>
<Tab value="pnpm">

```bash
# This project is not published to pnpm.
# Use Poetry or pip in a Python 3.10 environment instead.
git clone https://github.com/nsphung/pyspark-template.git
cd pyspark-template
pip install poetry
poetry install
```

</Tab>
<Tab value="yarn">

```bash
# This project is not published to Yarn.
# Use Poetry or pip in a Python 3.10 environment instead.
git clone https://github.com/nsphung/pyspark-template.git
cd pyspark-template
pip install poetry
poetry install
```

</Tab>
<Tab value="bun">

```bash
# This project is not published to Bun.
# Use Poetry or pip in a Python 3.10 environment instead.
git clone https://github.com/nsphung/pyspark-template.git
cd pyspark-template
pip install poetry
poetry install
```

</Tab>
</Tabs>

The runtime assumptions come directly from [README.md](https://github.com/nsphung/pyspark-template): Python 3.10, Java 11, Spark 3.5.6, and environment variables such as `JAVA_HOME`, `SPARK_HOME`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON`.

## Quick Start

The minimum working flow is the packaged CLI entry point declared in `pyproject.toml`:

```toml
[tool.poetry.scripts]
drugs_gen = "pyspark_template.jobs.drugs_gen:drugs_gen"
```

Run it with the sample datasets bundled in the repository:

```bash
poetry run drugs_gen \
  --drugs data/drugs.csv \
  --pubmed data/pubmed.csv \
  --clinicals_trials data/clinical_trials.csv \
  --output data/output/result.json
```

Expected output:

```text
Writing drugs result in file data/output/result.json
```

The generated JSON is an array of drug records. Each record contains:

- `atccode` and `drug` from `data/drugs.csv`
- `pubmeds`, collected from title matches in `data/pubmed.csv`
- `clinical_trials`, collected from title matches in `data/clinical_trials.csv`
- `journals`, a union of the journal/date pairs gathered from both sources

Example output excerpt:

```json
[
  {
    "atccode": "A01AD",
    "drug": "EPINEPHRINE",
    "pubmeds": [
      {
        "pubmed_id": "8",
        "pubmed_title": "Time to epinephrine treatment morality is associated with the risk of mortality in children who achieve sustained ROSC after traumatic out-of-hospital cardiac arrest.",
        "pubmed_date": "01/03/2020",
        "pubmed_journal": "The journal of allergy and clinical immunology. In practice"
      }
    ]
  }
]
```

## Key Features

- A real CLI entry point exposed through Poetry as `drugs_gen`
- One reusable Spark session factory with cached defaults in `pyspark_template/utils/spark.py`
- Small, composable DataFrame transforms with focused tests in `tests/transform/test_common.py`
- A single-file JSON writing pipeline that converts Spark output into a conventional API-style document
- Local development support through Poetry, Make targets, and Jupyter-friendly dependencies
- Compatibility with local execution and packaged distribution via `poetry build` and `spark-submit`

## Where To Go Next

<Cards>
  <Card title="Architecture" href="/docs/architecture">See how the reader, transform, Spark, and writer modules connect inside the job.</Card>
  <Card title="Core Concepts" href="/docs/spark-session">Start with the abstractions that shape the project: Spark sessions, title matching, and output generation.</Card>
  <Card title="API Reference" href="/docs/api-reference/jobs-drugs-gen">Review the exact signatures and import paths for every supported function.</Card>
</Cards>
