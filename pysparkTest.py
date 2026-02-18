from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").load("aisdk-2026-02-05.csv", header=True, inferSchema=True)
df = df.dropDuplicates()
# save to csv
df.coalesce(1).write.format("csv").option("header", "true").save("aisdk-2026-02-05.cleaned.csv")

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)