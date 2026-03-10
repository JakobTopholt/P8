from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852


def haversine_km(lat1, lon1, lat2, lon2):
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = (F.sin(d_lat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
         * F.sin(d_lon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1.0) - a))


def clean_head(df, base_margin, time_scale):
    """Check first 3 points per ship. Remove any that don't fit with the other two."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    df = df.withColumn("_row_num", F.row_number().over(w))

    # --- previous neighbor (lag 1) ---
    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)
    prev_ts  = F.lag("# Timestamp").over(w)

    time_h_prev = (F.col("# Timestamp").cast("long") - prev_ts.cast("long")) / 3600.0
    dist_prev   = haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    exp_km_prev = prev_sog * KNOTS_TO_KMH * time_h_prev
    margin_prev = base_margin * (1.0 + time_scale * time_h_prev)

    # --- next neighbor (lead 1) ---
    next_lat = F.lead("Latitude").over(w)
    next_lon = F.lead("Longitude").over(w)
    next_ts  = F.lead("# Timestamp").over(w)

    time_h_next = (next_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_next   = haversine_km(F.col("Latitude"), F.col("Longitude"), next_lat, next_lon)
    exp_km_next = F.col("SOG") * KNOTS_TO_KMH * time_h_next
    margin_next = base_margin * (1.0 + time_scale * time_h_next)

    # --- next-next neighbor (lead 2) — only used for P1 ---
    next2_lat = F.lead("Latitude", 2).over(w)
    next2_lon = F.lead("Longitude", 2).over(w)
    next2_ts  = F.lead("# Timestamp", 2).over(w)

    time_h_next2 = (next2_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_next2   = haversine_km(F.col("Latitude"), F.col("Longitude"), next2_lat, next2_lon)
    exp_km_next2 = F.col("SOG") * KNOTS_TO_KMH * time_h_next2
    margin_next2 = base_margin * (1.0 + time_scale * time_h_next2)

    reachable_prev  = F.coalesce(dist_prev  <= exp_km_prev  * margin_prev,  F.lit(False))
    reachable_next  = F.coalesce(dist_next  <= exp_km_next  * margin_next,  F.lit(False))
    reachable_next2 = F.coalesce(dist_next2 <= exp_km_next2 * margin_next2, F.lit(False))

    in_head = F.col("_row_num") <= 3
    is_p1   = F.col("_row_num") == 1
    is_last = next_ts.isNull()
    has_p3  = next2_ts.isNotNull()

    # P1: outlier if far from P2 AND far from P3 (need P3 to exist)
    outlier_p1 = is_p1 & ~reachable_next & ~reachable_next2 & has_p3

    # P2/P3: outlier if far from both prev and next (need both to exist)
    outlier_other = ~is_p1 & in_head & ~reachable_prev & ~reachable_next & ~is_last

    outlier = outlier_p1 | outlier_other

    return df.withColumn("_outlier", outlier).filter(~F.col("_outlier")).drop("_row_num", "_outlier")


def forward_pass(df, base_margin, time_scale):
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)
    prev_ts  = F.lag("# Timestamp").over(w)

    time_h = (F.col("# Timestamp").cast("long") - prev_ts.cast("long")) / 3600.0
    dist   = haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    exp_km = prev_sog * KNOTS_TO_KMH * time_h
    margin = base_margin * (1.0 + time_scale * time_h)

    is_first = prev_ts.isNull()
    in_range = dist <= exp_km * margin
    keep = F.coalesce(is_first | in_range, F.lit(False))

    return df.withColumn("_keep", keep).filter(F.col("_keep")).drop("_keep")


def remove_gps_outliers(df, base_margin=1.2, time_scale=0.3, max_passes=3):
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    # Phase 1: clean first 3 points per ship (bidirectional, single pass)
    df = clean_head(df, base_margin, time_scale)

    # Phase 2: forward pass on whole dataset (head now clean)
    prev_count = -1
    for i in range(max_passes):
        df = forward_pass(df, base_margin, time_scale)
        df = df.localCheckpoint(eager=True)
        curr_count = df.count()
        print(f"  Forward pass {i+1}: {curr_count} rows remaining")
        if curr_count == prev_count:
            break
        prev_count = curr_count

    return df