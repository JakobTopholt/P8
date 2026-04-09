from pyspark.sql import DataFrame
from pyspark.sql import functions as F


OUTPUT_COLUMNS = [
    "MMSI",
    "# Timestamp",
    "Type of mobile",
    "Latitude",
    "Longitude",
    "SOG",
    "COG",
    "Ship type",
]


def deduplicate_and_filter(df: DataFrame) -> DataFrame:
    # Keep only columns that survive into cleaned.csv before shuffle-heavy operations.
    df = df.select(*OUTPUT_COLUMNS)
    df = df.filter(F.col("Type of mobile") == "Class A")
    return df.dropDuplicates(OUTPUT_COLUMNS)
