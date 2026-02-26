from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import math


def trim_moving(
    df: DataFrame,
    time_threshold_seconds: int = 3600,
    distance_factor: float = 0.1,
    degree_threshold: float = 30.0,
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
    enriched = non_stationary.join(
        F.broadcast(avg_sog_by_ship_type), on="Ship type", how="left"
    ).withColumn(
        "_dist_threshold",
        F.coalesce(F.col("avg_SOG") * F.lit(distance_factor), F.lit(1.0)),
    )

    output_schema = enriched.schema          # applyInPandas needs the schema

    # --- greedy selection executed once per MMSI via Pandas UDF ------------
    def _greedy_trim(pdf):
        import numpy as np

        pdf = pdf.sort_values("# Timestamp").reset_index(drop=True)
        n = len(pdf)
        if n <= 2:
            return pdf

        keep = [False] * n
        keep[0] = True           # always keep first point
        keep[n - 1] = True       # always keep last  point
        last = 0                 # index of last kept point

        for i in range(1, n):
            # ---- time delta --------------------------------------------------
            dt = (pdf.at[i, "# Timestamp"] - pdf.at[last, "# Timestamp"]).total_seconds()

            # ---- haversine distance (NM) -------------------------------------
            try:
                lat1 = math.radians(float(pdf.at[last, "Latitude"]))
                lon1 = math.radians(float(pdf.at[last, "Longitude"]))
                lat2 = math.radians(float(pdf.at[i, "Latitude"]))
                lon2 = math.radians(float(pdf.at[i, "Longitude"]))
                dlat, dlon = lat2 - lat1, lon2 - lon1
                a = (math.sin(dlat / 2) ** 2
                     + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
                dist_nm = 2 * math.asin(math.sqrt(min(a, 1.0))) * 3440.065
            except (TypeError, ValueError):
                dist_nm = 0.0

            # ---- course (COG) drift ------------------------------------------
            try:
                drift = abs(float(pdf.at[i, "COG"]) - float(pdf.at[last, "COG"])) % 360
                if drift > 180:
                    drift = 360 - drift
            except (TypeError, ValueError):
                drift = 0.0

            # ---- distance threshold for this row -----------------------------
            dist_thr = pdf.at[i, "_dist_threshold"]
            if dist_thr is None or (isinstance(dist_thr, float) and np.isnan(dist_thr)):
                dist_thr = 1.0

            # ---- keep if ANY criterion fires ---------------------------------
            if dt >= time_threshold_seconds or dist_nm >= dist_thr or drift >= degree_threshold:
                keep[i] = True
                last = i

        return pdf.loc[keep].reset_index(drop=True)

    trimmed = (
        enriched
        .groupby("MMSI")
        .applyInPandas(_greedy_trim, output_schema)
    )

    # drop helper columns before returning
    trimmed = trimmed.drop("avg_SOG", "_dist_threshold")

    return trimmed.unionByName(stationary)