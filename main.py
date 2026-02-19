from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import os
import removeDuplications
import removeShiptypes
import trimStationary

input_file = "AISDATA/aisdk-2026-02-05.csv"
output_path = "AISDATA/aisdk-2026-02-05.cleaned.csv"

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin;' + os.environ.get('PATH', '')

start_time = time.time()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_file)

timestamp_col = "# Timestamp"  # adjust if your column name differs
df = df.withColumn(timestamp_col, F.to_timestamp(F.col(timestamp_col), "dd/MM/yyyy HH:mm:ss"))


df = removeDuplications.deduplicate_and_filter(df)
df = trimStationary.trim_stationary(df)
df = removeShiptypes.remove_shiptypes(df)

df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path)

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)
print("Count of rows after processing:", df.count())

spark.stop()
