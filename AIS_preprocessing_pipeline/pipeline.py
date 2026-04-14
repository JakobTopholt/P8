import glob
import os
import shutil
import time
from pathlib import Path

from .environment.hadoop_environment import configure_hadoop_environment
from .environment.java_environment import configure_java_environment
from .environment.spark_environment import (
    configure_pyspark_python,
    configure_spark_environment,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .steps import remove_duplicates
from .steps import remove_outliers
from .steps import remove_shiptypes
from .steps import ship_type
from .steps import trim_stationary

PROJECT_DIR = Path(__file__).resolve().parent.parent
AISDATA_DIR = PROJECT_DIR / "AISDATA"
RAW_DIR = AISDATA_DIR / "raw_AIS_files"
DEFAULT_DATE_TAG = "2026-02-05"


def _default_input_file() -> Path:
    """Find the raw CSV: try raw_AIS_files/ first, fall back to AISDATA/ root."""
    raw = RAW_DIR / f"aisdk-{DEFAULT_DATE_TAG}.csv"
    if raw.exists():
        return raw
    return AISDATA_DIR / f"aisdk-{DEFAULT_DATE_TAG}.csv"


DEFAULT_OUTPUT_PATH = AISDATA_DIR / f"aisdk-{DEFAULT_DATE_TAG}.cleaned.csv"


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(int(raw), minimum)
    except ValueError:
        return default


def run() -> None:
    input_file = Path(os.environ.get("AIS_INPUT_FILE", str(_default_input_file()))).expanduser()
    output_path = Path(os.environ.get("AIS_OUTPUT_PATH", str(DEFAULT_OUTPUT_PATH))).expanduser()
    local_cores = _env_int("SPARK_LOCAL_CORES", default=16, minimum=1)
    shuffle_partitions = _env_int("SPARK_SHUFFLE_PARTITIONS", default=64, minimum=8)
    input_partition_mb = _env_int("SPARK_INPUT_PARTITION_MB", default=64, minimum=16)
    output_partitions = _env_int("SPARK_OUTPUT_PARTITIONS", default=1, minimum=1)
    checkpoint_dir = PROJECT_DIR / "spark_temp" / "checkpoints"

    configure_java_environment(PROJECT_DIR, verbose=True)
    configure_hadoop_environment(PROJECT_DIR, verbose=True)
    configure_pyspark_python()
    configure_spark_environment(PROJECT_DIR)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()

    spark = (
        SparkSession.builder
        .master(f"local[{local_cores}]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", str(input_partition_mb * 1024 * 1024))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(checkpoint_dir))

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(str(input_file))
    )

    df = remove_duplicates.deduplicate_and_filter(df)

    timestamp_col = "# Timestamp"  # adjust if your column name differs
    timestamp_expr = F.coalesce(
        F.try_to_timestamp(F.col(timestamp_col), F.lit("dd/MM/yyyy HH:mm:ss")),
        F.try_to_timestamp(F.col(timestamp_col), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
        F.try_to_timestamp(F.col(timestamp_col)),
    )
    df = (
        df
        .withColumn(timestamp_col, timestamp_expr)
        .withColumn("Latitude", F.col("Latitude").cast("double"))
        .withColumn("Longitude", F.col("Longitude").cast("double"))
        .withColumn("SOG", F.col("SOG").cast("double"))
        .withColumn("COG", F.col("COG").cast("double"))
        .filter(F.col(timestamp_col).isNotNull())
        .filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull())
        .filter(F.col("SOG").isNotNull())
    )

    df = trim_stationary.trim_stationary(df)
    df = ship_type.fill_ship_type(df)
    df = ship_type.remove_undefined_ship_type(df)
    df = remove_shiptypes.remove_shiptypes(df)
    df = remove_outliers.remove_gps_outliers(df)
    df = df.select(*remove_duplicates.OUTPUT_COLUMNS)

    (
        df.coalesce(output_partitions)
        .write
        .format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(str(output_path))
    )

    elapsed_time = time.time() - start_time

    print("elapsed_time:", elapsed_time)
    if os.environ.get("PRINT_ROW_COUNT", "0") == "1":
        print("Count of rows after processing:", df.count())

    spark.stop()

    # Copy the Spark output to a clean preprocessed file
    preprocessed_dir = Path(os.environ.get(
        "AIS_PREPROCESSED_DIR",
        str(AISDATA_DIR / "preprocessed_AIS_files"),
    ))
    part_files = sorted(glob.glob(str(output_path / "part-*.csv")))
    if part_files:
        preprocessed_dir.mkdir(parents=True, exist_ok=True)
        date_tag = os.environ.get("AIS_DATE_TAG", input_file.stem.replace("aisdk-", ""))
        dest = preprocessed_dir / f"preprocessed_{date_tag}.csv"
        shutil.copy2(part_files[0], dest)
        print(f"[Clean] Copied to: {dest}")


if __name__ == "__main__":
    run()
