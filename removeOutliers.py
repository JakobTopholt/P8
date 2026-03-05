from pyspark.sql import functions as F
from math import radians, sin, cos, sqrt, atan2


def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def remove_gps_outliers(df, base_margin=1.2, time_scale=0.3):
    
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    output_cols = df.columns
    schema = df.schema

    def _process_mmsi(pdf):
        pdf = pdf.sort_values("# Timestamp").reset_index(drop=True)
        if pdf.empty:
            return pdf[output_cols]

        latitudes  = pdf["Latitude"].values
        longitudes = pdf["Longitude"].values
        speeds     = pdf["SOG"].values
        timestamps = pdf["# Timestamp"].astype("int64").values // 10**9
        row_count  = len(pdf)

        keep = [False] * row_count
        init_count = min(5, row_count)

        # ── Phase 1: cross-validate first 5 rows ──
        for i in range(init_count):
            is_valid = True
            for j in range(init_count):
                if i == j:
                    continue
                distance = _haversine(latitudes[i], longitudes[i], latitudes[j], longitudes[j])
                time_hours = abs(float(timestamps[j]) - float(timestamps[i])) / 3600.0
                speed_kmh = float(speeds[j]) * 1.852
                expected_distance = speed_kmh * time_hours
                margin = base_margin * (1.0 + time_scale * time_hours)
                if distance > expected_distance * margin:
                    is_valid = False
                    break
            if is_valid:
                keep[i] = True

        # Guarantee at least one approved anchor
        if not any(keep[:init_count]):
            keep[0] = True

        # Last approved index so far
        last_approved = max(i for i in range(init_count) if keep[i])

        # ── Phase 2: sequential scan ──
        for i in range(init_count, row_count):
            distance = _haversine(latitudes[last_approved], longitudes[last_approved], latitudes[i], longitudes[i])
            time_hours = (float(timestamps[i]) - float(timestamps[last_approved])) / 3600.0
            prev_speed_kmh = float(speeds[last_approved]) * 1.852
            expected_distance = prev_speed_kmh * time_hours
            margin = base_margin * (1.0 + time_scale * time_hours)

            if distance <= expected_distance * margin:
                keep[i] = True
                last_approved = i

        return pdf.loc[keep, output_cols]

    result = df.groupby("MMSI").applyInPandas(_process_mmsi, schema=schema)
    return result