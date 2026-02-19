from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def trim_stationary(df: DataFrame):

    stationary = df.filter(F.col("SOG") == 0).withColumn(
        "_hour", F.date_trunc("hour", F.col("# Timestamp"))
    )

    non_stationary = df.filter(F.col("SOG") != 0)

    # Keep one stationary row per MMSI per hour (earliest timestamp)
    w = Window.partitionBy(F.col("MMSI"), F.col("_hour")).orderBy(F.col("# Timestamp").asc())

    stationary_trimmed = (
        stationary.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_hour")
    )

    # Return all non-stationary rows + trimmed stationary rows
    return non_stationary.unionByName(stationary_trimmed)