import os
import time
from pathlib import Path

import removeDuplications
import removeShiptypes
import trimStationary
from java_environment import configure_java_environment
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent
INPUT_FILE = PROJECT_DIR / "data" / "raw" / "aisdk-2026-02-05.csv"
OUTPUT_PATH = PROJECT_DIR / "data" / "processed" / "aisdk-2026-02-05.cleaned.csv"


def prepend_path(path: Path) -> None:
    path_str = str(path)
    current_path = os.environ.get("PATH", "")
    path_entries = current_path.split(os.pathsep) if current_path else []
    if path_str not in path_entries:
        os.environ["PATH"] = path_str + (os.pathsep + current_path if current_path else "")


def configure_windows_hadoop() -> None:
    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    if Path(hadoop_home).is_dir():
        os.environ["HADOOP_HOME"] = hadoop_home
        prepend_path(Path(hadoop_home) / "bin")
    else:
        print(
            f"Warning: HADOOP_HOME '{hadoop_home}' was not found. "
            "Set HADOOP_HOME if your Windows Spark setup needs winutils."
        )


configure_java_environment(PROJECT_DIR)
configure_windows_hadoop()

start_time = time.time()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(str(INPUT_FILE))

timestamp_col = "# Timestamp"  # adjust if your column name differs
df = df.withColumn(timestamp_col, F.to_timestamp(F.col(timestamp_col), "dd/MM/yyyy HH:mm:ss"))


df = removeDuplications.deduplicate_and_filter(df)
df = trimStationary.trim_stationary(df)
df = removeShiptypes.remove_shiptypes(df)

df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(str(OUTPUT_PATH))

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)
print("Count of rows after processing:", df.count())

spark.stop()
