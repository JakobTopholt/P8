from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852
# GPS accuracy floor: points within this distance are never outliers
MIN_ALLOWED_KM = 0.05  # 50 meters


def haversine_km(lat1, lon1, lat2, lon2):
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = (F.sin(d_lat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
         * F.sin(d_lon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1.0) - a))


def _allowed_km(sog1, sog2, time_h, base_margin, time_scale):
    """Expected reachable distance using the max of two SOG values, with a GPS floor."""
    best_sog = F.greatest(sog1, sog2)
    exp_km = best_sog * KNOTS_TO_KMH * time_h
    margin = base_margin * (1.0 + time_scale * time_h)
    return F.greatest(exp_km * margin, F.lit(MIN_ALLOWED_KM))


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
    allowed_prev = _allowed_km(prev_sog, F.col("SOG"), time_h_prev, base_margin, time_scale)

    # --- next neighbor (lead 1) ---
    next_lat = F.lead("Latitude").over(w)
    next_lon = F.lead("Longitude").over(w)
    next_sog = F.lead("SOG").over(w)
    next_ts  = F.lead("# Timestamp").over(w)

    time_h_next = (next_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_next   = haversine_km(F.col("Latitude"), F.col("Longitude"), next_lat, next_lon)
    allowed_next = _allowed_km(F.col("SOG"), next_sog, time_h_next, base_margin, time_scale)

    # --- next-next neighbor (lead 2) — only used for P1 ---
    next2_lat = F.lead("Latitude", 2).over(w)
    next2_lon = F.lead("Longitude", 2).over(w)
    next2_sog = F.lead("SOG", 2).over(w)
    next2_ts  = F.lead("# Timestamp", 2).over(w)

    time_h_next2 = (next2_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_next2   = haversine_km(F.col("Latitude"), F.col("Longitude"), next2_lat, next2_lon)
    allowed_next2 = _allowed_km(F.col("SOG"), next2_sog, time_h_next2, base_margin, time_scale)

    reachable_prev  = F.coalesce(dist_prev  <= allowed_prev,  F.lit(False))
    reachable_next  = F.coalesce(dist_next  <= allowed_next,  F.lit(False))
    reachable_next2 = F.coalesce(dist_next2 <= allowed_next2, F.lit(False))

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


def bidirectional_pass(df, base_margin, time_scale):
    """Remove a point only if it is unreachable from BOTH its previous and next neighbour."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    # --- previous neighbor ---
    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)
    prev_ts  = F.lag("# Timestamp").over(w)

    time_h_prev = (F.col("# Timestamp").cast("long") - prev_ts.cast("long")) / 3600.0
    dist_prev   = haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    allowed_prev = _allowed_km(prev_sog, F.col("SOG"), time_h_prev, base_margin, time_scale)

    # --- next neighbor ---
    next_lat = F.lead("Latitude").over(w)
    next_lon = F.lead("Longitude").over(w)
    next_sog = F.lead("SOG").over(w)
    next_ts  = F.lead("# Timestamp").over(w)

    time_h_next = (next_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_next   = haversine_km(F.col("Latitude"), F.col("Longitude"), next_lat, next_lon)
    allowed_next = _allowed_km(F.col("SOG"), next_sog, time_h_next, base_margin, time_scale)

    reachable_from_prev = F.coalesce(dist_prev <= allowed_prev, F.lit(True))
    reachable_from_next = F.coalesce(dist_next <= allowed_next, F.lit(True))

    is_first = prev_ts.isNull()
    is_last  = next_ts.isNull()

    # Keep if: first point, last point, or reachable from at least one neighbor
    keep = is_first | is_last | reachable_from_prev | reachable_from_next

    return df.withColumn("_keep", keep).filter(F.col("_keep")).drop("_keep")


def skip_neighbor_pass(df, base_margin, time_scale):
    """Remove points that only survive because a bad immediate neighbor shields them.

    Checks lag(2) and lead(2) — if a point can't reach the track when
    skipping one point in either direction, it is part of an isolated
    pair (or larger cluster) and gets removed.

    At track edges (near start/end), falls back to checking the
    available skip-neighbor plus the immediate neighbor on the other side.

    Designed to run iteratively: each pass peels off the outer layer of
    a cluster, exposing interior bad points for the next pass.
    """
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    # --- immediate neighbors (for edge fallback) ---
    prev_ts  = F.lag("# Timestamp").over(w)
    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)

    time_h_prev = (F.col("# Timestamp").cast("long") - prev_ts.cast("long")) / 3600.0
    dist_prev   = haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    allowed_prev = _allowed_km(prev_sog, F.col("SOG"), time_h_prev, base_margin, time_scale)

    next_ts  = F.lead("# Timestamp").over(w)
    next_lat = F.lead("Latitude").over(w)
    next_lon = F.lead("Longitude").over(w)
    next_sog = F.lead("SOG").over(w)

    time_h_next = (next_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_next   = haversine_km(F.col("Latitude"), F.col("Longitude"), next_lat, next_lon)
    allowed_next = _allowed_km(F.col("SOG"), next_sog, time_h_next, base_margin, time_scale)

    reachable_prev = F.coalesce(dist_prev <= allowed_prev, F.lit(True))
    reachable_next = F.coalesce(dist_next <= allowed_next, F.lit(True))

    # --- skip-one backward: lag(2) ---
    p2_ts  = F.lag("# Timestamp", 2).over(w)
    p2_lat = F.lag("Latitude", 2).over(w)
    p2_lon = F.lag("Longitude", 2).over(w)
    p2_sog = F.lag("SOG", 2).over(w)

    time_h_p2 = (F.col("# Timestamp").cast("long") - p2_ts.cast("long")) / 3600.0
    dist_p2   = haversine_km(p2_lat, p2_lon, F.col("Latitude"), F.col("Longitude"))
    allowed_p2 = _allowed_km(p2_sog, F.col("SOG"), time_h_p2, base_margin, time_scale)

    # --- skip-one forward: lead(2) ---
    n2_ts  = F.lead("# Timestamp", 2).over(w)
    n2_lat = F.lead("Latitude", 2).over(w)
    n2_lon = F.lead("Longitude", 2).over(w)
    n2_sog = F.lead("SOG", 2).over(w)

    time_h_n2 = (n2_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
    dist_n2   = haversine_km(F.col("Latitude"), F.col("Longitude"), n2_lat, n2_lon)
    allowed_n2 = _allowed_km(F.col("SOG"), n2_sog, time_h_n2, base_margin, time_scale)

    reachable_p2 = F.coalesce(dist_p2 <= allowed_p2, F.lit(True))
    reachable_n2 = F.coalesce(dist_n2 <= allowed_n2, F.lit(True))

    has_p2   = p2_ts.isNotNull()
    has_n2   = n2_ts.isNotNull()
    has_prev = prev_ts.isNotNull()
    has_next = next_ts.isNotNull()

    # Interior: both skip-neighbors exist — isolated if neither is reachable
    interior_isolated = has_p2 & has_n2 & ~reachable_p2 & ~reachable_n2

    # Near start (no lag 2): unreachable from next(1) AND unreachable from lead(2)
    start_isolated = ~has_p2 & has_next & has_n2 & ~reachable_next & ~reachable_n2

    # Near end (no lead 2): unreachable from prev(1) AND unreachable from lag(2)
    end_isolated = has_prev & has_p2 & ~has_n2 & ~reachable_prev & ~reachable_p2

    isolated = interior_isolated | start_isolated | end_isolated

    return df.withColumn("_iso", isolated).filter(~F.col("_iso")).drop("_iso")


def remove_gps_outliers(df, base_margin=1.2, time_scale=0.3, max_passes=3):
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    # Phase 1: clean first 3 points per ship (bidirectional, single pass)
    df = clean_head(df, base_margin, time_scale)

    # Phase 2: bidirectional pass — only remove if BOTH immediate neighbors disagree
    prev_count = -1
    for i in range(max_passes):
        df = bidirectional_pass(df, base_margin, time_scale)
        df = df.checkpoint(eager=True)
        curr_count = df.count()
        print(f"  Bidirectional pass {i+1}: {curr_count} rows remaining")
        if curr_count == prev_count:
            break
        prev_count = curr_count

    # Phase 3: skip-neighbor pass — catch paired/clustered outliers that shield each other
    # A point must be reachable from the broader track (lag 2 or lead 2),
    # not just from its immediate neighbor which might itself be bad.
    # Runs iteratively: each pass peels off the outer layer of clusters.
    total_skip_removed = 0
    for i in range(max_passes * 3):
        df = skip_neighbor_pass(df, base_margin, time_scale)
        df = df.checkpoint(eager=True)
        after = df.count()
        removed = curr_count - after
        total_skip_removed += removed
        print(f"  Skip-neighbor pass {i+1}: removed {removed}")
        if removed == 0:
            break
        curr_count = after

    print(f"  Skip-neighbor total: removed {total_skip_removed}")

    return df
