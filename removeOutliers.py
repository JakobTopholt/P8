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

def remove_gps_outliers(df, speed_margin=2.0, median_max_km=1500):
    df = df.withColumn("Latitude", F.col("Latitude").cast("double")) \
           .withColumn("Longitude", F.col("Longitude").cast("double")) \
           .withColumn("SOG", F.col("SOG").cast("double"))

    cols = df.columns
    df = df.withColumn("_ts", F.unix_timestamp(F.col("# Timestamp")))

    # Step 1: Median filter — removes wild GPS glitches (wrong hemisphere, etc.)
    for _ in range(2):
        medians = df.groupBy("MMSI").agg(
            F.percentile_approx("Latitude", 0.5).alias("_med_lat"),
            F.percentile_approx("Longitude", 0.5).alias("_med_lon")
        )
        df = df.join(F.broadcast(medians), "MMSI")
        df = df.withColumn("_med_dist",
            haversine(F.col("Latitude"), F.col("Longitude"), F.col("_med_lat"), F.col("_med_lon"))
        )
        df = df.filter(F.col("_med_dist") <= median_max_km).select(cols + ["_ts"])

    # Step 2: Speed check — removes subtle outliers using bilateral prev+next
    w = Window.partitionBy("MMSI").orderBy("_ts")
    for _ in range(3):
        df = df.withColumn("_prev_lat", F.lag("Latitude").over(w)) \
               .withColumn("_prev_lon", F.lag("Longitude").over(w)) \
               .withColumn("_prev_sog", F.lag("SOG").over(w)) \
               .withColumn("_prev_ts", F.lag("_ts").over(w)) \
               .withColumn("_next_lat", F.lead("Latitude").over(w)) \
               .withColumn("_next_lon", F.lead("Longitude").over(w)) \
               .withColumn("_next_sog", F.lead("SOG").over(w)) \
               .withColumn("_next_ts", F.lead("_ts").over(w))

        df = df.withColumn("_actual_prev",
            haversine(F.col("_prev_lat"), F.col("_prev_lon"), F.col("Latitude"), F.col("Longitude"))
        ).withColumn("_actual_next",
            haversine(F.col("Latitude"), F.col("Longitude"), F.col("_next_lat"), F.col("_next_lon"))
        )

        df = df.withColumn("_expect_prev",
            expected_distance(F.col("_prev_sog"), F.col("SOG"), F.col("_prev_ts"), F.col("_ts"))
        ).withColumn("_expect_next",
            expected_distance(F.col("SOG"), F.col("_next_sog"), F.col("_ts"), F.col("_next_ts"))
        )

        prev_fail = F.col("_actual_prev") > F.col("_expect_prev") * speed_margin
        next_fail = F.col("_actual_next") > F.col("_expect_next") * speed_margin
        has_prev = F.col("_prev_lat").isNotNull()
        has_next = F.col("_next_lat").isNotNull()

        is_outlier = (
            (has_prev & has_next & prev_fail & next_fail) |
            (has_prev & ~has_next & prev_fail) |
            (~has_prev & has_next & next_fail)
        )

        df = df.filter(~is_outlier).select(cols + ["_ts"])

    df = df.select(cols)
    return df