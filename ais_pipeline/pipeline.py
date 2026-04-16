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
DEFAULT_INPUT_FILE = AISDATA_DIR / "aisdk-2026-02-05.csv"


def default_cleaned_output_path(input_file: Path) -> Path:
    """Return the default cleaned CSV path for an input AIS CSV file."""
    if input_file.suffix.lower() == ".csv":
        return input_file.with_name(f"{input_file.stem}.cleaned{input_file.suffix}")
    return input_file.with_name(f"{input_file.name}.cleaned.csv")


def resolve_output_path(input_file: Path) -> Path:
    """Resolve AIS_OUTPUT_PATH or derive it from AIS_INPUT_FILE."""
    output_path = os.environ.get("AIS_OUTPUT_PATH")
    if output_path:
        return Path(output_path).expanduser()
    return default_cleaned_output_path(input_file)


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(int(raw), minimum)
    except ValueError:
        return default


def _replace_existing_output(output_path: Path) -> None:
    if not output_path.exists():
        return
    if output_path.is_dir():
        shutil.rmtree(output_path)
    else:
        output_path.unlink()


def _finalize_single_csv_output(spark_output_dir: Path, output_path: Path) -> None:
    part_files = sorted(spark_output_dir.glob("part-*.csv"))
    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected exactly one Spark CSV part in {spark_output_dir}, "
            f"found {len(part_files)}. Set AIS_OUTPUT_AS_DIRECTORY=1 if you need "
            "multi-part Spark output."
        )

    _replace_existing_output(output_path)
    shutil.move(str(part_files[0]), str(output_path))
    shutil.rmtree(spark_output_dir)


def run() -> None:
    input_file = Path(os.environ.get("AIS_INPUT_FILE", str(DEFAULT_INPUT_FILE))).expanduser()
    output_path = resolve_output_path(input_file)
    local_cores = _env_int("SPARK_LOCAL_CORES", default=4, minimum=1)
    shuffle_partitions = _env_int("SPARK_SHUFFLE_PARTITIONS", default=64, minimum=8)
    input_partition_mb = _env_int("SPARK_INPUT_PARTITION_MB", default=64, minimum=16)
    output_partitions = _env_int("SPARK_OUTPUT_PARTITIONS", default=1, minimum=1)
    output_as_directory = os.environ.get("AIS_OUTPUT_AS_DIRECTORY", "0") == "1"
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

    if output_as_directory:
        (
            df.coalesce(output_partitions)
            .write
            .format("csv")
            .option("header", "true")
            .mode("overwrite")
            .save(str(output_path))
        )
    else:
        if output_partitions != 1:
            print(
                "Ignoring SPARK_OUTPUT_PARTITIONS for single-file CSV output. "
                "Set AIS_OUTPUT_AS_DIRECTORY=1 to keep Spark's multi-part directory output."
            )

        temp_output_dir = output_path.parent / f".{output_path.name}.spark-tmp-{os.getpid()}"
        if temp_output_dir.exists():
            shutil.rmtree(temp_output_dir)

        try:
            (
                df.coalesce(1)
                .write
                .format("csv")
                .option("header", "true")
                .mode("overwrite")
                .save(str(temp_output_dir))
            )
            _finalize_single_csv_output(temp_output_dir, output_path)
        finally:
            if temp_output_dir.exists():
                shutil.rmtree(temp_output_dir)

    print("cleaned_output:", output_path)

    elapsed_time = time.time() - start_time

    print("elapsed_time:", elapsed_time)
    if os.environ.get("PRINT_ROW_COUNT", "0") == "1":
        print("Count of rows after processing:", df.count())

    spark.stop()


if __name__ == "__main__":
    run()
