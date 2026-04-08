""" GPS Outlier Detector for AIS ship tracking data

Removes position points that are physically impossible given a ship's speed (SOG).
Runs three phases in sequence:

Phase 1 — clean_head:
    Checks the first 3 points of each ship's track. Bidirectional (Phase 2) always    
    keeps the first and last point because they have no prev/next neighbor, so this       phase specifically handles P1/P2/P3 by checking them against each other.

Phase 2 — bidirectional_pass (single pass):
    Removes a point if it is unreachable from BOTH its immediate prev and next
    neighbor. Handles the common case: one isolated outlier surrounded by good points.

Phase 3 — skip_neighbor_pass (up to max_passes iterations):
    Handles paired outliers — two consecutive bad points that shield each other from Phase 2. Runs iteratively to peel through
    multi-point outliers, one layer at a time.
    
Reachability is defined by allowed_km(): max distance a ship could travel given its
reported SOG, elapsed time, a safety margin, and a 50 m GPS accuracy floor. """

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


def allowed_km(sog1, sog2, time_h, base_margin, time_scale):
    best_sog = F.greatest(sog1, sog2)
    exp_km = best_sog * KNOTS_TO_KMH * time_h
    margin = base_margin * (1.0 + time_scale * time_h)
    return F.greatest(exp_km * margin, F.lit(MIN_ALLOWED_KM))


def reach(w, offset, base_margin, time_scale, null_means=True):
    """Reachability check for a neighbor at `offset` (negative=lag, positive=lead)."""
    abs_off = abs(offset)
    fn = F.lead if offset > 0 else F.lag

    neighbor_ts  = fn("# Timestamp", abs_off).over(w)
    neighbor_lat = fn("Latitude",    abs_off).over(w)
    neighbor_lon = fn("Longitude",   abs_off).over(w)
    neighbor_sog = fn("SOG",         abs_off).over(w)

    if offset > 0:  # forward neighbor
        time_h  = (neighbor_ts.cast("long") - F.col("# Timestamp").cast("long")) / 3600.0
        dist    = haversine_km(F.col("Latitude"), F.col("Longitude"), neighbor_lat, neighbor_lon)
        allowed = allowed_km(F.col("SOG"), neighbor_sog, time_h, base_margin, time_scale)
    else:           # backward neighbor
        time_h  = (F.col("# Timestamp").cast("long") - neighbor_ts.cast("long")) / 3600.0
        dist    = haversine_km(neighbor_lat, neighbor_lon, F.col("Latitude"), F.col("Longitude"))
        allowed = allowed_km(neighbor_sog, F.col("SOG"), time_h, base_margin, time_scale)

    has       = neighbor_ts.isNotNull()
    reachable = F.coalesce(dist <= allowed, F.lit(null_means))
    return has, reachable


# ── Phase 1: Clean head ────────────────────────────────────────────

def clean_head(df, base_margin, time_scale):
    """Check first 3 points per ship. Remove any that don't fit with the other two."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    df = df.withColumn("row_num", F.row_number().over(w))

    _,        reach_prev  = reach(w, -1, base_margin, time_scale, null_means=False)
    has_next, reach_next  = reach(w,  1, base_margin, time_scale, null_means=False)
    has_next2, reach_next2 = reach(w, 2, base_margin, time_scale, null_means=False)

    is_p1 = F.col("row_num") == 1
    is_p2 = F.col("row_num") == 2
    is_p3 = F.col("row_num") == 3

    # P1: outlier if far from P2 AND far from P3 (need P3 to exist)
    outlier_p1 = is_p1 & ~reach_next & ~reach_next2 & has_next2
    # P2: outlier if far from both P1 and P3 (need P3 to exist)
    outlier_p2 = is_p2 & ~reach_prev & ~reach_next & has_next
    # P3: outlier if far from both P2 and P4 (need P4 to exist)
    outlier_p3 = is_p3 & ~reach_prev & ~reach_next & has_next

    outlier = outlier_p1 | outlier_p2 | outlier_p3
    return df.withColumn("is_outlier", outlier).filter(~F.col("is_outlier")).drop("row_num", "is_outlier")


# ── Phase 2: Bidirectional pass ────────────────────────────────────

def bidirectional_pass(df, base_margin, time_scale):
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    has_prev, reach_prev = reach(w, -1, base_margin, time_scale, null_means=True)
    has_next, reach_next = reach(w,  1, base_margin, time_scale, null_means=True)

    keep = ~has_prev | ~has_next | reach_prev | reach_next
    return df.withColumn("_k", keep).filter(F.col("_k")).drop("_k")


# ── Phase 3: Skip-neighbor pass ───────────────────────────────────

def skip_neighbor_pass(df, base_margin, time_scale):
    
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    has_prev, reach_prev = reach(w, -1, base_margin, time_scale, null_means=True)
    has_next, reach_next = reach(w,  1, base_margin, time_scale, null_means=True)
    has_p2,   reach_p2   = reach(w, -2, base_margin, time_scale, null_means=True)
    has_n2,   reach_n2   = reach(w,  2, base_margin, time_scale, null_means=True)

    # Interior: both skip-neighbors exist but neither is reachable
    interior = has_p2 & has_n2 & ~reach_p2 & ~reach_n2
    # Near start (no lag 2): unreachable from next(1) AND lead(2)
    start    = ~has_p2 & has_next & has_n2 & ~reach_next & ~reach_n2
    # Near end (no lead 2): unreachable from prev(1) AND lag(2)
    end      = has_prev & has_p2 & ~has_n2 & ~reach_prev & ~reach_p2

    isolated = interior | start | end
    return df.withColumn("_iso", isolated).filter(~F.col("_iso")).drop("_iso")


# ── Orchestrator ───────────────────────────────────────────────────

def run_iterative(df, pass_fn, base_margin, time_scale, max_iter, label):
    """Run a pass function iteratively until convergence, with checkpointing."""
    prev_count = df.count()
    for i in range(max_iter):
        df = pass_fn(df, base_margin, time_scale)
        df = df.checkpoint(eager=True)
        curr_count = df.count()
        removed_rows = prev_count - curr_count
        print(f"  {label} {i+1}: {curr_count} rows ({removed_rows} removed)")
        if curr_count == prev_count:
            break
        prev_count = curr_count
    return df, curr_count


def outlier_detector(df, base_margin=1.2, time_scale=0.3, max_passes=3):
    df = (df
          .withColumn("Latitude",  F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG",       F.col("SOG").cast("double")))

    # Phase 1: clean first 3 points
    df = clean_head(df, base_margin, time_scale)

    # Phase 2: bidirectional — single pass
    prev_count = df.count()
    df = bidirectional_pass(df, base_margin, time_scale)
    df = df.checkpoint(eager=True)
    new_count = df.count()
    print(f"  Bidirectional: {new_count} rows ({prev_count - new_count} removed)")

    # Phase 3: skip-neighbor — iteratively peel cluster layers
    df, _ = run_iterative(
        df, skip_neighbor_pass, base_margin, time_scale, max_passes, "Skip-neighbor"
    )

    return df
