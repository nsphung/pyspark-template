---
title: "Reuse The Code In Notebooks"
description: "Use the template's packaged modules inside Jupyter notebooks instead of copying Spark logic into ad hoc cells."
---

The repository includes `notebooks/` and lists Jupyter-related dependencies in the Poetry development group. That signals an intended workflow: use notebooks for exploration, but import the reusable code from `pyspark_template` instead of rebuilding the pipeline by hand.

## Problem

Notebook-only PySpark development tends to drift:

- Spark setup gets duplicated across notebooks.
- Small transformation fixes never make it back into the package.
- The logic you validated interactively diverges from the CLI that runs in CI or production.

## Solution

Treat the notebook as a consumer of the package, not the home of the logic.

<Steps>
<Step>
### Install the project in its Poetry environment

```bash
pip install poetry
poetry install
poetry run python -m ipykernel install --user --name pyspark-template
```

This uses the dependencies already declared in `pyproject.toml`, including `findspark` and `ipykernel` in the dev group.

</Step>
<Step>
### Start Spark through the shared helper

In the notebook, import the packaged session factory:

```python
from pyspark_template.utils.spark import get_spark_session

spark = get_spark_session()
```

Now the notebook and the CLI share the same Spark baseline instead of maintaining separate builder code.

</Step>
<Step>
### Pull in the same transforms used by the CLI

```python
from pyspark_template.readers.csv import read_csv
from pyspark_template.transform.common import prefix_cols, prepared_title_array

pubmed_df = (
    read_csv(spark, "../data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)

pubmed_df.select("pubmed_id", "pubmed_title_array").show(truncate=False)
```

That lets you inspect intermediate state without changing the production code path.

</Step>
</Steps>

## A Realistic Notebook Pattern

One useful notebook flow is to prototype new matching rules while keeping the stable parts imported:

```python
import pyspark.sql.functions as f

from pyspark_template.readers.csv import read_csv
from pyspark_template.transform.common import prefix_cols, prepared_title_array

drugs_df = read_csv(spark, "../data/drugs.csv")
pubmed_df = (
    read_csv(spark, "../data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)

candidate_matches = drugs_df.join(
    pubmed_df,
    f.array_contains(pubmed_df.pubmed_title_array, f.upper(drugs_df.drug)),
    "outer",
)

candidate_matches.show(truncate=False)
```

Once the idea is sound, move the new logic into `pyspark_template/transform/common.py` or a new transform module, then keep the notebook focused on inspection and validation.

## Why This Matters

This repository is a template. Its main value is not just the sample drug-matching job, but the workflow discipline it models:

- package code goes in `pyspark_template/`
- runnable orchestration goes in `jobs/`
- exploration happens in `notebooks/`
- tests lock in behavior for reusable transforms

That separation is the difference between a notebook proving that something works once and a codebase that other developers can run repeatedly.

<Callout type="warn">Do not paste evolving transformation logic into notebooks and leave the packaged module untouched. The next `poetry run drugs_gen` invocation will still execute the code in `pyspark_template/jobs/drugs_gen.py`, not the experimental cells you changed interactively.</Callout>

If you need to persist a notebook-proven change, the safest sequence is:

1. move the transformation into `pyspark_template/transform/`
2. add or update a test in `tests/transform/`
3. rerun `poetry run drugs_gen` to verify the packaged job still behaves as expected
