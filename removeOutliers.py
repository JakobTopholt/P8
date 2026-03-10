from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852


def _haversine_km(lat1, lon1, lat2, lon2):
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = (F.sin(d_lat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
         * F.sin(d_lon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1.0) - a))


def _filter_one_pass(df, base_margin, time_scale):
    """Remove rows that are unreachable from their immediate predecessor."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)
    prev_ts  = F.lag("# Timestamp").over(w)

    time_h = (F.col("# Timestamp").cast("long") - prev_ts.cast("long")) / 3600.0
    dist   = _haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    exp_km = prev_sog * KNOTS_TO_KMH * time_h
    margin = base_margin * (1.0 + time_scale * time_h)

    is_first = prev_ts.isNull()
    in_range = dist <= exp_km * margin
    keep = F.coalesce(is_first | in_range, F.lit(False))

    return df.withColumn("_keep", keep).filter(F.col("_keep")).drop("_keep")


def remove_gps_outliers(df, base_margin=1.2, time_scale=0.3, max_passes=5):
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    prev_count = -1
    for i in range(max_passes):
        df = _filter_one_pass(df, base_margin, time_scale)
        df = df.localCheckpoint(eager=True)
        curr_count = df.count()
        print(f"  Outlier pass {i+1}: {curr_count} rows remaining")
        if curr_count == prev_count:
            break
        prev_count = curr_count

    return df