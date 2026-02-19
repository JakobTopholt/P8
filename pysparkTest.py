from pyspark.sql import SparkSession
import time
import os

# Set HADOOP_HOME environment - so windows can run Spark without errors
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin;' + os.environ.get('PATH', '')

start_time = time.time()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").load("aisdk-2026-02-05.csv", header=True, inferSchema=True)
df = df.dropDuplicates()
df = df.drop("Navigational status", "Heading", "ROT", "IMO", "Callsign", 
        "Name", "Cargo type", "Width", "Length", "Draught", "Destination", "ETA", 
        "Data source type", "A", "B", "C", "D", "Type of position fixing device")

df = df.filter(df["Type of mobile"]  == "Class A")


df.write.format("csv").option("header", "true").mode("overwrite").save("aisdk-2026-02-05.cleaned.csv")
#df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("aisdk-2026-02-05.cleaned.csv")

elapsed_time = time.time() - start_time

print("elapsed_time:", elapsed_time)