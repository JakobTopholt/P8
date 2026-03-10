from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import os
import removeDuplications
import removeShiptypes
import trimStationary
import ship_type
import trim_moving
import sys
import removeOutliers
import platform

input_file = "AISDATA/aisdk-2026-02-05.csv"
output_path = "AISDATA/aisdk-2026-02-05.cleaned.csv"

if platform.system() == "Windows":
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['PATH'] = r'C:\hadoop\bin;' + os.environ.get('PATH', '')

# Ensure Spark workers use the same Python interpreter (with pandas/pyarrow)
# On Windows, paths with spaces break PySpark worker launches, so use the
# short (8.3) path which contains no spaces.
python_exec = sys.executable
if platform.system() == "Windows":
    import ctypes
    buf = ctypes.create_unicode_buffer(260)
    if ctypes.windll.kernel32.GetShortPathNameW(python_exec, buf, 260):
        python_exec = buf.value

os.environ['PYSPARK_PYTHON'] = python_exec
os.environ['PYSPARK_DRIVER_PYTHON'] = python_exec

start_time = time.time()

spark = (SparkSession.builder
    .config("spark.local.dir", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_temp"))
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_file)

timestamp_col = "# Timestamp"  # adjust if your column name differs
df = df.withColumn(timestamp_col, F.to_timestamp(F.col(timestamp_col), "dd/MM/yyyy HH:mm:ss"))


df = removeDuplications.deduplicate_and_filter(df)
df = trimStationary.trim_stationary(df)
df = ship_type.fill_ship_type(df)
df = ship_type.remove_undefined_ship_type(df)
df = removeShiptypes.remove_shiptypes(df)
df = removeOutliers.remove_gps_outliers(df)
#df = trim_moving.trim_moving(df)


df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path)

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)
# print("Count of rows after processing:", df.count())

spark.stop()
