import json
import os
import shutil

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, spark_partition_id, struct, to_json


def write_json_array(df: DataFrame, output_file: str) -> None:
    df.select(to_json(struct(*df.columns)).alias("json")).groupBy(
        spark_partition_id()
    ).agg(collect_list("json").alias("json_list")).select(
        col("json_list").cast("string")
    ).write.mode(
        "overwrite"
    ).text(
        output_file
    )


def write_json_array_pd(df: DataFrame, output_file: str) -> None:
    with open(output_file, "w") as outfile:
        json.dump(df.toPandas().to_dict(orient="records"), outfile, indent=4)


def write_single_json(json_dir: str, output_json: str) -> None:
    # Iterate directory
    for path in os.listdir(json_dir):
        # check if current path is a file
        if (
            os.path.isfile(os.path.join(json_dir, path))
            and ".txt" in path
            and ".crc" not in path
        ):
            # Opening JSON file
            with open(os.path.join(json_dir, path), "r") as openfile:

                # Reading from json file
                json_object = json.load(openfile)
                # Writing to sample.json
                with open(output_json, "w") as outfile:
                    outfile.write(json.dumps(json_object, indent=4))


def write_json(df: DataFrame, path: str, output_file: str) -> None:
    # Sortie un JSON drug <->
    write_json_array(df, path)
    write_single_json(path, output_file)
    shutil.rmtree(path)
