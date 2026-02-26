from pyspark.sql import Window
from pyspark.sql import functions as F

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return R * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

def expected_distance(sog1, sog2, ts1, ts2):
    avg_speed_kmh = ((sog1 + sog2) / 2) * 1.852
    time_hours = (ts2 - ts1) / 3600
    return avg_speed_kmh * time_hours

def remove_gps_outliers(df, speed_margin=2.0):
    df = df.withColumn("Latitude", F.col("Latitude").cast("double")) \
           .withColumn("Longitude", F.col("Longitude").cast("double")) \
           .withColumn("SOG", F.col("SOG").cast("double"))

    cols = df.columns
    df = df.withColumn("_ts", F.unix_timestamp(F.col("# Timestamp")))

    w = Window.partitionBy("MMSI").orderBy("_ts")
    df = df.withColumn("_rn", F.row_number().over(w))

    first5 = df.filter(F.col("_rn") <= 5)
    totals = first5.groupBy("MMSI").agg(
        F.sum("Latitude").alias("_sum_lat"), F.sum("Longitude").alias("_sum_lon"),
        F.count("*").alias("_cnt")
    )
    first5 = first5.join(F.broadcast(totals), "MMSI")
    first5 = first5.withColumn("_loo_lat",
        F.when(F.col("_cnt") > 1, (F.col("_sum_lat") - F.col("Latitude")) / (F.col("_cnt") - 1))
         .otherwise(F.col("Latitude"))
    ).withColumn("_loo_lon",
        F.when(F.col("_cnt") > 1, (F.col("_sum_lon") - F.col("Longitude")) / (F.col("_cnt") - 1))
         .otherwise(F.col("Longitude"))
    )
    first5 = first5.withColumn("_loo_dist",
        haversine(F.col("Latitude"), F.col("Longitude"), F.col("_loo_lat"), F.col("_loo_lon"))
    )
    first5_clean = first5.filter(F.col("_loo_dist") <= 50).select(cols)

    rest = df.filter(F.col("_rn") > 5)
    for _ in range(3):
        w_rest = Window.partitionBy("MMSI").orderBy("_ts")
        rest = rest.withColumn("_prev_lat", F.lag("Latitude").over(w_rest)) \
                   .withColumn("_prev_lon", F.lag("Longitude").over(w_rest)) \
                   .withColumn("_prev_sog", F.lag("SOG").over(w_rest)) \
                   .withColumn("_prev_ts", F.lag("_ts").over(w_rest)) \
                   .withColumn("_prev2_lat", F.lag("Latitude", 2).over(w_rest)) \
                   .withColumn("_prev2_lon", F.lag("Longitude", 2).over(w_rest)) \
                   .withColumn("_prev2_sog", F.lag("SOG", 2).over(w_rest)) \
                   .withColumn("_prev2_ts", F.lag("_ts", 2).over(w_rest)) \
                   .withColumn("_next_lat", F.lead("Latitude").over(w_rest)) \
                   .withColumn("_next_lon", F.lead("Longitude").over(w_rest)) \
                   .withColumn("_next_sog", F.lead("SOG").over(w_rest)) \
                   .withColumn("_next_ts", F.lead("_ts").over(w_rest))

        rest = rest.withColumn("_actual_prev",
            haversine(F.col("_prev_lat"), F.col("_prev_lon"), F.col("Latitude"), F.col("Longitude"))
        ).withColumn("_actual_prev2",
            haversine(F.col("_prev2_lat"), F.col("_prev2_lon"), F.col("Latitude"), F.col("Longitude"))
        ).withColumn("_actual_next",
            haversine(F.col("Latitude"), F.col("Longitude"), F.col("_next_lat"), F.col("_next_lon"))
        )

        rest = rest.withColumn("_expect_prev",
            expected_distance(F.col("_prev_sog"), F.col("SOG"), F.col("_prev_ts"), F.col("_ts"))
        ).withColumn("_expect_prev2",
            expected_distance(F.col("_prev2_sog"), F.col("SOG"), F.col("_prev2_ts"), F.col("_ts"))
        ).withColumn("_expect_next",
            expected_distance(F.col("SOG"), F.col("_next_sog"), F.col("_ts"), F.col("_next_ts"))
        )

        prev_fail = F.col("_actual_prev") > F.col("_expect_prev") * speed_margin
        prev2_fail = F.col("_actual_prev2") > F.col("_expect_prev2") * speed_margin
        next_fail = F.col("_actual_next") > F.col("_expect_next") * speed_margin
        has_prev = F.col("_prev_lat").isNotNull()
        has_prev2 = F.col("_prev2_lat").isNotNull()
        has_next = F.col("_next_lat").isNotNull()

        is_outlier = (
            (has_prev & has_next & prev_fail & next_fail) |
            (has_prev & has_prev2 & prev_fail & prev2_fail) |
            (has_prev & ~has_next & prev_fail) |
            (~has_prev & has_next & next_fail)
        )

        rest = rest.filter(~is_outlier).select(cols + ["_ts"])

    df = first5_clean.unionByName(rest.select(cols))
    return df