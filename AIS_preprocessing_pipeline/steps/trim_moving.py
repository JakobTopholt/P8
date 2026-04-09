from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import math


def trim_moving(
    df: DataFrame,
    time_threshold_seconds: int = 3600,
    distance_factor: float = 0.1,
    degree_threshold: float = 10.0,
):
    """
    Trim moving-vessel AIS data with a greedy per-MMSI algorithm.

    A point is kept whenever **any** of the following is true
    (compared to the last *kept* point):

    1. Time elapsed  >= `time_threshold_seconds`  (default 3600 s = 1 h)
    2. Haversine distance >= `avg_SOG * distance_factor` NM
       (threshold scales with the average max-SOG of the ship type)
    3. COG drift     >= `degree_threshold` degrees  (default 30°)

    The first and last point of every MMSI are always retained.
    Stationary rows (SOG == 0) are passed through unchanged.
    """

    stationary = df.filter(F.col("SOG") == 0)
    non_stationary = df.filter(F.col("SOG") != 0)

    # --- average of per-MMSI max SOG, grouped by ship type ----------------
    max_sog_by_mmsi = (
        non_stationary
        .groupBy("MMSI", "Ship type")
        .agg(F.max("SOG").alias("max_SOG"))
    )
    avg_sog_by_ship_type = (
        max_sog_by_mmsi
        .groupBy("Ship type")
        .agg(F.avg("max_SOG").alias("avg_SOG"))
    )

    # join back & compute a per-row distance threshold (NM)
    base_columns = non_stationary.columns
    enriched = non_stationary.join(
        F.broadcast(avg_sog_by_ship_type), on="Ship type", how="left"
    ).withColumn(
        "_dist_threshold",
        F.coalesce(F.col("avg_SOG") * F.lit(distance_factor), F.lit(1.0)),
    ).select(
        *base_columns, "_dist_threshold"
    )

    output_schema = enriched.schema

    # Streaming greedy selection: keeps O(1) state per MMSI instead of
    # materializing each full MMSI group as a Pandas frame.
    def _greedy_trim_iter(pdf_iter):
        import pandas as pd

        def _to_float(value, default=0.0):
            try:
                number = float(value)
                if math.isnan(number):
                    return default
                return number
            except (TypeError, ValueError):
                return default

        columns = None
        out_rows = []
        flush_size = 50000

        # Per-MMSI streaming state.
        current_mmsi = None
        last_kept_ts = None
        last_kept_lat = None
        last_kept_lon = None
        last_kept_cog = None
        last_seen_row = None
        last_seen_emitted = False

        def finalize_current_mmsi():
            nonlocal last_seen_emitted
            if last_seen_row is not None and not last_seen_emitted:
                out_rows.append(last_seen_row)
                last_seen_emitted = True

        for pdf in pdf_iter:
            if pdf.empty:
                continue

            if columns is None:
                columns = list(pdf.columns)
                mmsi_idx = columns.index("MMSI")
                ts_idx = columns.index("# Timestamp")
                lat_idx = columns.index("Latitude")
                lon_idx = columns.index("Longitude")
                cog_idx = columns.index("COG")
                thr_idx = columns.index("_dist_threshold")

            for row in pdf.itertuples(index=False, name=None):
                mmsi = row[mmsi_idx]
                ts = row[ts_idx]
                lat = _to_float(row[lat_idx], default=0.0)
                lon = _to_float(row[lon_idx], default=0.0)
                cog = _to_float(row[cog_idx], default=0.0)
                dist_thr = _to_float(row[thr_idx], default=1.0)

                # New MMSI starts: finalize previous and seed state with first point.
                if current_mmsi is None or mmsi != current_mmsi:
                    finalize_current_mmsi()
                    current_mmsi = mmsi
                    last_kept_ts = ts
                    last_kept_lat = lat
                    last_kept_lon = lon
                    last_kept_cog = cog
                    last_seen_row = row
                    out_rows.append(row)  # always keep first point
                    last_seen_emitted = True
                else:
                    last_seen_row = row
                    last_seen_emitted = False

                    # ---- time delta -------------------------------------------
                    try:
                        dt = (ts - last_kept_ts).total_seconds()
                    except Exception:
                        dt = 0.0
                    if dt < 0:
                        dt = 0.0

                    # ---- haversine distance (NM) ------------------------------
                    lat1 = math.radians(last_kept_lat)
                    lon1 = math.radians(last_kept_lon)
                    lat2 = math.radians(lat)
                    lon2 = math.radians(lon)
                    dlat, dlon = lat2 - lat1, lon2 - lon1
                    a = (
                        math.sin(dlat / 2) ** 2
                        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
                    )
                    dist_nm = 2 * math.asin(math.sqrt(min(a, 1.0))) * 3440.065

                    # ---- course (COG) drift -----------------------------------
                    drift = abs(cog - last_kept_cog) % 360
                    if drift > 180:
                        drift = 360 - drift

                    # ---- keep if ANY criterion fires --------------------------
                    if (
                        dt >= time_threshold_seconds
                        or dist_nm >= dist_thr
                        or drift >= degree_threshold
                    ):
                        out_rows.append(row)
                        last_seen_emitted = True
                        last_kept_ts = ts
                        last_kept_lat = lat
                        last_kept_lon = lon
                        last_kept_cog = cog

                if len(out_rows) >= flush_size:
                    yield pd.DataFrame(out_rows, columns=columns)
                    out_rows = []

        finalize_current_mmsi()
        if out_rows:
            yield pd.DataFrame(out_rows, columns=columns)

    trimmed = (
        enriched
        .repartition("MMSI")
        .sortWithinPartitions("MMSI", "# Timestamp")
        .mapInPandas(_greedy_trim_iter, output_schema)
    )

    # drop helper columns before returning
    trimmed = trimmed.drop("_dist_threshold")

    return trimmed.unionByName(stationary)
