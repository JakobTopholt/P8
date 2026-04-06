import os
import time
from pathlib import Path

import removeOutliers
import removeDuplications
import removeShiptypes
import ship_type
import trim_moving
import trimStationary

from environment_setup.hadoop_environment import configure_hadoop_environment
from environment_setup.java_environment import configure_java_environment
from environment_setup.spark_environment import (
    configure_pyspark_python,
    configure_spark_environment,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent
AISDATA_DIR = PROJECT_DIR / "AISDATA"
DEFAULT_INPUT_FILE = AISDATA_DIR / "aisdk-2026-02-05.csv"
DEFAULT_OUTPUT_PATH = AISDATA_DIR / "aisdk-2026-02-05.cleaned.csv"


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(int(raw), minimum)
    except ValueError:
        return default


INPUT_FILE = Path(os.environ.get("AIS_INPUT_FILE", str(DEFAULT_INPUT_FILE))).expanduser()
OUTPUT_PATH = Path(os.environ.get("AIS_OUTPUT_PATH", str(DEFAULT_OUTPUT_PATH))).expanduser()
LOCAL_CORES = _env_int("SPARK_LOCAL_CORES", default=8, minimum=1)
SHUFFLE_PARTITIONS = _env_int("SPARK_SHUFFLE_PARTITIONS", default=64, minimum=8)
INPUT_PARTITION_MB = _env_int("SPARK_INPUT_PARTITION_MB", default=64, minimum=16)
OUTPUT_PARTITIONS = _env_int("SPARK_OUTPUT_PARTITIONS", default=1, minimum=1)
CHECKPOINT_DIR = PROJECT_DIR / "spark_temp" / "checkpoints"

configure_java_environment(PROJECT_DIR, verbose=True)
configure_hadoop_environment(PROJECT_DIR, verbose=True)
configure_pyspark_python()
configure_spark_environment(PROJECT_DIR)

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

start_time = time.time()

spark = (SparkSession.builder
    .master(f"local[{LOCAL_CORES}]")
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    .config("spark.default.parallelism", str(SHUFFLE_PARTITIONS))
    .config("spark.sql.files.maxPartitionBytes", str(INPUT_PARTITION_MB * 1024 * 1024))
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())

CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
spark.sparkContext.setCheckpointDir(str(CHECKPOINT_DIR))

df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load(str(INPUT_FILE)))


df = removeDuplications.deduplicate_and_filter(df)

timestamp_col = "# Timestamp"  # adjust if your column name differs
timestamp_expr = F.coalesce(
    F.try_to_timestamp(F.col(timestamp_col), F.lit("dd/MM/yyyy HH:mm:ss")),
    F.try_to_timestamp(F.col(timestamp_col), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
    F.try_to_timestamp(F.col(timestamp_col)),
)
df = (df
      .withColumn(timestamp_col, timestamp_expr)
      .withColumn("Latitude", F.col("Latitude").cast("double"))
      .withColumn("Longitude", F.col("Longitude").cast("double"))
      .withColumn("SOG", F.col("SOG").cast("double"))
      .withColumn("COG", F.col("COG").cast("double"))
      .filter(F.col(timestamp_col).isNotNull())
      .filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull())
      .filter(F.col("SOG").isNotNull()))

df = trimStationary.trim_stationary(df)
df = ship_type.fill_ship_type(df)
df = ship_type.remove_undefined_ship_type(df)
df = removeShiptypes.remove_shiptypes(df)
df = removeOutliers.remove_gps_outliers(df)
df = df.select(*removeDuplications.OUTPUT_COLUMNS)

df.coalesce(OUTPUT_PARTITIONS).write.format("csv").option("header", "true").mode("overwrite").save(str(OUTPUT_PATH))

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)
if os.environ.get("PRINT_ROW_COUNT", "0") == "1":
    print("Count of rows after processing:", df.count())

spark.stop()
