import os
import sys
import time
from pathlib import Path

import removeDuplications
import removeShiptypes
import ship_type
import trim_moving
import trimStationary
from hadoop_environment import configure_hadoop_environment
from java_environment import configure_java_environment
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent
AISDATA_DIR = PROJECT_DIR / "AISDATA"
INPUT_FILE = AISDATA_DIR / "aisdk-2026-02-05.csv"
OUTPUT_PATH = AISDATA_DIR / "aisdk-2026-02-05.cleaned.csv"


def configure_pyspark_python() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


configure_java_environment(PROJECT_DIR)
configure_hadoop_environment(PROJECT_DIR)
configure_pyspark_python()

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

start_time = time.time()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(str(INPUT_FILE))

timestamp_col = "# Timestamp"  # adjust if your column name differs
df = df.withColumn(timestamp_col, F.to_timestamp(F.col(timestamp_col), "dd/MM/yyyy HH:mm:ss"))


df = removeDuplications.deduplicate_and_filter(df)
df = trimStationary.trim_stationary(df)
df = ship_type.fill_ship_type(df)
df = ship_type.remove_undefined_ship_type(df)
df = removeShiptypes.remove_shiptypes(df)
df = trim_moving.trim_moving(df)

df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(str(OUTPUT_PATH))

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)
if os.environ.get("PRINT_ROW_COUNT", "0") == "1":
    print("Count of rows after processing:", df.count())

spark.stop()
