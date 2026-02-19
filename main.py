from pyspark.sql import SparkSession
import time
import os
import removeDuplications
import removeShiptypes

input_file = "AISDATA/aisdk-2026-02-05.csv"
output_path = "aisdk-2026-02-05.cleaned.csv"

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin;' + os.environ.get('PATH', '')

start_time = time.time()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true").load(input_file)

df = removeDuplications.remove_duplications(df)
df = removeShiptypes.remove_shiptypes(df)

df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path)

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)

spark.stop()
