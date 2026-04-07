from pyspark.sql import functions as F
from pyspark.sql.window import Window

EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852
MIN_ALLOWED_KM = 0.05  # 50 m GPS accuracy floor


# ── Shared helpers ──────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = (F.sin(d_lat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
         * F.sin(d_lon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1.0) - a))


def _allowed_km(sog1, sog2, time_h, base_margin, time_scale):
    best_sog = F.greatest(sog1, sog2)
    exp_km = best_sog * KNOTS_TO_KMH * time_h
    margin = base_margin * (1.0 + time_scale * time_h)
    return F.greatest(exp_km * margin, F.lit(MIN_ALLOWED_KM))


def _reach(w, offset, base_margin, time_scale, null_means=True):
    """Reachability check for a neighbor at `offset` (negative=lag, positive=lead).

    Returns (has_neighbor, is_reachable) column expressions.
    `null_means` controls what reachable defaults to when the neighbor doesn't exist.
    """
    abs_off = abs(offset)
    fn = F.lead if offset > 0 else F.lag

    nb_ts  = fn("# Timestamp", abs_off).over(w)
    nb_lat = fn("Latitude",    abs_off).over(w)
    nb_lon = fn("Longitude",   abs_off).over(w)
    nb_sog = fn("SOG",         abs_off).over(w)

    if offset > 0:  # forward neighbor
        time_h  = (nb_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
        dist    = haversine_km(F.col("Latitude"), F.col("Longitude"), nb_lat, nb_lon)
        allowed = _allowed_km(F.col("SOG"), nb_sog, time_h, base_margin, time_scale)
    else:           # backward neighbor
        time_h  = (F.col("# Timestamp").cast("long") - nb_ts.cast("long")) / 3600.0
        dist    = haversine_km(nb_lat, nb_lon, F.col("Latitude"), F.col("Longitude"))
        allowed = _allowed_km(nb_sog, F.col("SOG"), time_h, base_margin, time_scale)

    has       = nb_ts.isNotNull()
    reachable = F.coalesce(dist <= allowed, F.lit(null_means))
    return has, reachable


# ── Phase 1: Clean head ────────────────────────────────────────────

def _clean_head(df, base_margin, time_scale):
    """Check first 3 points per ship. Remove any that don't fit with the other two."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    df = df.withColumn("_rn", F.row_number().over(w))

    _,        reach_prev  = _reach(w, -1, base_margin, time_scale, null_means=False)
    has_next, reach_next  = _reach(w,  1, base_margin, time_scale, null_means=False)
    has_next2, reach_next2 = _reach(w, 2, base_margin, time_scale, null_means=False)

    in_head = F.col("_rn") <= 3
    is_p1   = F.col("_rn") == 1

    # P1: outlier if far from P2 AND far from P3 (need P3 to exist)
    outlier_p1 = is_p1 & ~reach_next & ~reach_next2 & has_next2
    # P2/P3: outlier if far from both prev and next (need both to exist)
    outlier_other = ~is_p1 & in_head & ~reach_prev & ~reach_next & has_next

    outlier = outlier_p1 | outlier_other
    return df.withColumn("_out", outlier).filter(~F.col("_out")).drop("_rn", "_out")


# ── Phase 2: Bidirectional pass ────────────────────────────────────

def _bidirectional_pass(df, base_margin, time_scale):
    """Remove a point only if unreachable from BOTH its previous and next neighbour."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    has_prev, reach_prev = _reach(w, -1, base_margin, time_scale, null_means=True)
    has_next, reach_next = _reach(w,  1, base_margin, time_scale, null_means=True)

    keep = ~has_prev | ~has_next | reach_prev | reach_next
    return df.withColumn("_k", keep).filter(F.col("_k")).drop("_k")


# ── Phase 3: Skip-neighbor pass ───────────────────────────────────

def _skip_neighbor_pass(df, base_margin, time_scale):
    """Remove points shielded by a bad immediate neighbor.

    Checks lag(2)/lead(2). At track edges, falls back to the available
    skip-neighbor plus the immediate neighbor on the other side.
    """
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    has_prev, reach_prev = _reach(w, -1, base_margin, time_scale, null_means=True)
    has_next, reach_next = _reach(w,  1, base_margin, time_scale, null_means=True)
    has_p2,   reach_p2   = _reach(w, -2, base_margin, time_scale, null_means=True)
    has_n2,   reach_n2   = _reach(w,  2, base_margin, time_scale, null_means=True)

    # Interior: both skip-neighbors exist but neither is reachable
    interior = has_p2 & has_n2 & ~reach_p2 & ~reach_n2
    # Near start (no lag 2): unreachable from next(1) AND lead(2)
    start    = ~has_p2 & has_next & has_n2 & ~reach_next & ~reach_n2
    # Near end (no lead 2): unreachable from prev(1) AND lag(2)
    end      = has_prev & has_p2 & ~has_n2 & ~reach_prev & ~reach_p2

    isolated = interior | start | end
    return df.withColumn("_iso", isolated).filter(~F.col("_iso")).drop("_iso")


# ── Orchestrator ───────────────────────────────────────────────────

def _run_iterative(df, pass_fn, base_margin, time_scale, max_iter, label):
    """Run a pass function iteratively until convergence, with checkpointing."""
    prev_count = df.count()
    for i in range(max_iter):
        df = pass_fn(df, base_margin, time_scale)
        df = df.checkpoint(eager=True)
        curr_count = df.count()
        removed = prev_count - curr_count
        print(f"  {label} {i+1}: {curr_count} rows ({removed} removed)")
        if curr_count == prev_count:
            break
        prev_count = curr_count
    return df, curr_count


def remove_gps_outliers(df, base_margin=1.2, time_scale=0.3, max_passes=3):
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    # Phase 1: clean first 3 points
    df = _clean_head(df, base_margin, time_scale)

    # Phase 2: bidirectional — only repeat if previous pass removed something
    df, count = _run_iterative(
        df, _bidirectional_pass, base_margin, time_scale, max_passes, "Bidirectional"
    )

    # Phase 3: skip-neighbor — iteratively peel cluster layers
    df, _ = _run_iterative(
        df, _skip_neighbor_pass, base_margin, time_scale, max_passes * 3, "Skip-neighbor"
    )

    return df
