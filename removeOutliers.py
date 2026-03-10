from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852


def _haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance between two points as a Spark Column (km)."""
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = (F.sin(d_lat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
         * F.sin(d_lon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1.0) - a))


def _filter_outlier_pass(df, mmsi_window, base_margin, time_scale):
    """One pass: compare each row to its predecessor and drop outliers."""
    prev_lat = F.lag("Latitude").over(mmsi_window)
    prev_lon = F.lag("Longitude").over(mmsi_window)
    prev_sog = F.lag("SOG").over(mmsi_window)
    prev_ts  = F.lag("# Timestamp").over(mmsi_window)

    time_diff_hours = (F.col("# Timestamp").cast("long") - prev_ts.cast("long")) / 3600.0
    distance_km     = _haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    expected_km     = prev_sog * KNOTS_TO_KMH * time_diff_hours
    margin          = base_margin * (1.0 + time_scale * time_diff_hours)

    is_first_in_group = prev_ts.isNull()
    is_within_range   = distance_km <= expected_km * margin

    return (df
            .withColumn("_keep", is_first_in_group | is_within_range)
            .filter(F.col("_keep"))
            .drop("_keep"))


def remove_gps_outliers(df, base_margin=1.2, time_scale=0.3):
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    mmsi_window = Window.partitionBy("MMSI").orderBy("# Timestamp")
    df = _filter_outlier_pass(df, mmsi_window, base_margin, time_scale)
    return df