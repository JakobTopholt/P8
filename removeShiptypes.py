from pyspark.sql.functions import col, sum, when

def remove_shiptypes(df, sog_threshold=50, min_datapoints=25):
    df = df.filter(df["Ship type"] != "SAR")
    
    undefined_df = df.filter((col("Ship type") == "Undefined") | (col("Ship type") == ""))
    
    mmsi_to_remove = undefined_df.groupBy("MMSI").agg(
        sum(when(col("SOG").cast("double") > sog_threshold, 1).otherwise(0)).alias("high_sog_count")
    ).filter(
        col("high_sog_count") >= min_datapoints
    ).select("MMSI")
    
    df = df.join(mmsi_to_remove, on="MMSI", how="left_anti")
    
    return df