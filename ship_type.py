from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def fill_ship_type(df: DataFrame):
    window = Window.partitionBy("MMSI")

    # Replace "Undefined" string with actual null first
    df = df.withColumn(
        "Ship type",
        F.when(F.col("Ship type") == "Undefined", None).otherwise(F.col("Ship type"))
    )

    # Now fill nulls using first non-null value within the same MMSI group
    df = df.withColumn(
        "Ship type",
        F.coalesce(
            F.col("Ship type"),
            F.first(F.col("Ship type"), ignorenulls=True).over(window)
        )
    )

    return df

def remove_undefined_ship_type(df: DataFrame):
    return df.filter(F.col("Ship type") != "Undefined")