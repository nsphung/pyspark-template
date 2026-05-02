---
title: "Customize Spark Configuration"
description: "Adjust the template's Spark configuration for local testing, notebooks, or larger jobs."
---

The default Spark settings are sensible for a small local template, but they are still only defaults. This guide shows how to take control of `SparkConf` without rewriting the rest of the pipeline.

## Problem

You want to keep the template's structure, but you need to:

- reduce shuffle partitions for tiny local runs
- use a different app name or session lifecycle in a notebook
- avoid accidental reuse of a cached session with the wrong configuration

## Solution

Use `spark_conf_default` from `pyspark_template/utils/spark.py` as your starting point, then pass the resulting `SparkConf` into `get_spark_session`.

<Steps>
<Step>
### Build a custom Spark configuration

```python
from pyspark_template.utils.spark import spark_conf_default

conf = spark_conf_default(shuffle_partitions="4")
```

This keeps the same serializer and Arrow settings as the template while shrinking the shuffle width for a small local dataset.

</Step>
<Step>
### Create the Spark session before anything else does

```python
from pyspark_template.utils.spark import get_spark_session

spark = get_spark_session(conf)
print(spark.conf.get("spark.sql.shuffle.partitions"))
```

Expected output:

```text
4
```

</Step>
<Step>
### Reuse the session in your pipeline

```python
from pyspark_template.readers.csv import read_csv
from pyspark_template.transform.common import prefix_cols, prepared_title_array

pubmed_df = (
    read_csv(spark, "data/pubmed.csv")
    .transform(lambda df: prefix_cols(df, "pubmed"))
    .transform(lambda df: prepared_title_array(df, "pubmed_title"))
)
```

The rest of the repository works unchanged because every downstream function consumes a normal Spark DataFrame.

</Step>
</Steps>

## Notebook Pattern

For a notebook session, make the custom configuration the first cell that touches Spark:

```python
from pyspark_template.utils.spark import get_spark_session, spark_conf_default

spark = get_spark_session(spark_conf_default(shuffle_partitions="8"))
```

Then build readers and transforms in later cells. This avoids a common trap where an earlier exploratory cell calls `get_spark_session()` with defaults and locks in that cached session.

## Cluster Submission Pattern

If you package the project with `poetry build`, you can still keep the same configuration approach in code and let `spark-submit` manage the cluster-level options:

```bash
spark-submit \
  --master yarn \
  --conf spark.sql.shuffle.partitions=200 \
  dist/pyspark_template-0.2.1-py3-none-any.whl
```

The repository README explicitly points to this distribution model for distributed execution. In that setup, prefer cluster-managed settings for executor sizing and memory, and reserve `spark_conf_default` for code-level defaults that should always apply.

## Trade-Offs In Practice

<Accordions>
<Accordion title="Code-level config vs deployment-level config">
Putting configuration in Python keeps the default behavior close to the business logic, which is useful for a template and for local reproducibility. It also means a new developer can inspect one file, `pyspark_template/utils/spark.py`, and understand the runtime baseline. The trade-off is that some settings really belong to the deployment environment, especially master URL, executor memory, and dynamic allocation.

For cluster jobs, it is usually cleaner to let `spark-submit` or the scheduler own those values while the code owns only durable application defaults. That split keeps infrastructure concerns outside the package and makes it easier to reuse the same wheel in multiple environments.
</Accordion>
<Accordion title="Caching convenience vs iterative experimentation">
Caching makes the template pleasant to use in a script because the session behaves like a singleton for the current process. That same behavior can slow down experimentation when you are iterating on configuration values in a notebook. If you need fresh settings repeatedly, restart the kernel or explicitly stop the session before recreating it.

Otherwise, you may think a config change took effect when you are still running against the first cached session. That is a good trade if your priority is stable, repeatable execution, but it is a cost when you are exploring performance tuning interactively.
</Accordion>
</Accordions>

<Callout type="warn">The helper API exposes only one adjustable parameter, `shuffle_partitions`. Any other configuration changes require editing `pyspark_template/utils/spark.py` or building a new `SparkConf` manually before the first call to `get_spark_session`.</Callout>
