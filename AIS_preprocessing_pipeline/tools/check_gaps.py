"""
Check the N biggest consecutive gaps in cleaned data and classify each as
legitimate (ship speed can explain it) or suspicious (potential missed outlier).

Usage:
    python -m ais_pipeline.tools.check_gaps --top 1000
    python -m ais_pipeline.tools.check_gaps --top 1000 --suspicious-only
"""

import argparse
import sys
from pathlib import Path

# Add repo root to path so this script can be run both directly and as a module.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ais_pipeline.environment.hadoop_environment import configure_hadoop_environment
from ais_pipeline.environment.java_environment import configure_java_environment
from ais_pipeline.environment.spark_environment import (
    configure_pyspark_python,
    configure_spark_environment,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

AISDATA_DIR = REPO_ROOT / "AISDATA"
CHECKPOINT_DIR = REPO_ROOT / "spark_temp" / "checkpoints"
EARTH_RADIUS_KM = 6371.0
KNOTS_TO_KMH = 1.852


def haversine_km(lat1, lon1, lat2, lon2):
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = (F.sin(d_lat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
         * F.sin(d_lon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * F.atan2(F.sqrt(a), F.sqrt(F.lit(1.0) - a))


def fmt_dist(km):
    if km is None:
        return "     N/A"
    if km < 1.0:
        return f"{km * 1000:>7.1f} m"
    return f"{km:>7.3f} km"


def build_spark():
    configure_java_environment(REPO_ROOT, verbose=False)
    configure_hadoop_environment(REPO_ROOT, verbose=False)
    configure_pyspark_python()
    configure_spark_environment(REPO_ROOT)

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("check_gaps")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(
        description="Check N biggest gaps in cleaned data for missed outliers"
    )
    parser.add_argument(
        "--input",
        default=str(AISDATA_DIR / "aisdk-2026-02-05.cleaned.csv"),
        help="Path to cleaned CSV (Spark output directory)",
    )
    parser.add_argument(
        "--top", type=int, default=1000,
        help="Check this many biggest gaps (default: 1000)",
    )
    parser.add_argument(
        "--suspicious-only", action="store_true",
        help="Only print gaps classified as suspicious",
    )
    args = parser.parse_args()

    spark = build_spark()

    print(f"Reading cleaned data from: {args.input}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(args.input)
    )
    df = (
        df.withColumn("Latitude", F.col("Latitude").cast("double"))
          .withColumn("Longitude", F.col("Longitude").cast("double"))
          .withColumn("SOG", F.col("SOG").cast("double"))
          .withColumn("# Timestamp",
                      F.coalesce(
                          F.try_to_timestamp(F.col("# Timestamp"),
                                             F.lit("dd/MM/yyyy HH:mm:ss")),
                          F.try_to_timestamp(F.col("# Timestamp"),
                                             F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
                          F.try_to_timestamp(F.col("# Timestamp")),
                      ))
    )

    total_rows = df.count()
    print(f"Total rows: {total_rows:,}\n")

    # ── Compute every consecutive gap ──
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)
    prev_ts  = F.lag("# Timestamp").over(w)

    next_lat = F.lead("Latitude").over(w)
    next_lon = F.lead("Longitude").over(w)
    next_sog = F.lead("SOG").over(w)
    next_ts  = F.lead("# Timestamp").over(w)

    gaps = (
        df
        .withColumn("_prev_lat", prev_lat)
        .withColumn("_prev_lon", prev_lon)
        .withColumn("_prev_sog", prev_sog)
        .withColumn("_prev_ts", prev_ts)
        .withColumn("_next_lat", next_lat)
        .withColumn("_next_lon", next_lon)
        .withColumn("_next_sog", next_sog)
        .withColumn("_next_ts", next_ts)
    )

    # Filter after window columns are materialized
    gaps = (
        gaps
        .filter(F.col("_prev_ts").isNotNull())
        .withColumn("_dist_km",
                    haversine_km(prev_lat, prev_lon,
                                 F.col("Latitude"), F.col("Longitude")))
        .withColumn("_time_s",
                    F.col("# Timestamp").cast("long") - prev_ts.cast("long"))
        .withColumn("_time_h",
                    F.col("_time_s") / 3600.0)
        # Speed needed to cover the gap
        .withColumn("_needed_kn",
                    F.when(F.col("_time_h") > 0,
                           F.col("_dist_km") / (F.col("_time_h") * KNOTS_TO_KMH)))
        # Max SOG of the two endpoints
        .withColumn("_max_sog",
                    F.greatest(F.col("_prev_sog"), F.col("SOG")))
        # Ratio: needed speed / reported SOG  (>1 = suspicious)
        .withColumn("_speed_ratio",
                    F.when(F.col("_max_sog") > 0,
                           F.col("_needed_kn") / F.col("_max_sog")))
        # Distance to next point (to check if THIS point is an outlier)
        .withColumn("_dist_next_km",
                    haversine_km(F.col("Latitude"), F.col("Longitude"),
                                 next_lat, next_lon))
        .withColumn("_time_next_s",
                    F.when(next_ts.isNotNull(),
                           next_ts.cast("long") - F.col("# Timestamp").cast("long")))
        # Distance from prev to next (skip current point)
        .withColumn("_skip_dist_km",
                    F.when(next_lat.isNotNull(),
                           haversine_km(prev_lat, prev_lon, next_lat, next_lon)))
        .withColumn("_skip_time_h",
                    F.when(next_ts.isNotNull(),
                           (next_ts.cast("long") - prev_ts.cast("long")) / 3600.0))
        .withColumn("_skip_needed_kn",
                    F.when((F.col("_skip_time_h").isNotNull()) & (F.col("_skip_time_h") > 0),
                           F.col("_skip_dist_km") / (F.col("_skip_time_h") * KNOTS_TO_KMH)))
    )

    # ── Classify each gap ──
    # LEGITIMATE: needed speed <= max_sog * 1.5 (ship could realistically cover it)
    # SUSPICIOUS: needed speed >> reported SOG, OR skip-distance is much smaller
    #             (removing this point would fix the gap)
    gaps = gaps.withColumn(
        "_verdict",
        F.when(F.col("_time_s") == 0, F.lit("DUPLICATE_TS"))
         .when(F.col("_speed_ratio") <= 1.5, F.lit("OK_SPEED_MATCHES"))
         .when(
             # Current point is outlier if: gap is big AND skipping it makes prev→next normal
             (F.col("_skip_needed_kn").isNotNull()) &
             (F.col("_skip_needed_kn") < F.col("_max_sog") * 1.5) &
             (F.col("_speed_ratio") > 2.0),
             F.lit("SUSPICIOUS_SKIP_FIXES"))
         .when(F.col("_speed_ratio") > 3.0, F.lit("SUSPICIOUS_VERY_FAST"))
         .when(F.col("_speed_ratio") > 1.5, F.lit("BORDERLINE"))
         .otherwise(F.lit("OK"))
    )

    # ── Get top N biggest gaps ──
    top_gaps = (
        gaps.orderBy(F.col("_dist_km").desc())
        .limit(args.top)
    )

    top_gaps.cache()
    rows = top_gaps.collect()

    # ── Summarize verdicts ──
    verdict_counts = {}
    for r in rows:
        v = r["_verdict"]
        verdict_counts[v] = verdict_counts.get(v, 0) + 1

    print(f"Top {args.top} biggest gaps — verdict summary:")
    print("=" * 50)
    for v, c in sorted(verdict_counts.items(), key=lambda x: -x[1]):
        print(f"  {v:<30} {c:>5}")
    print("=" * 50)

    # ── Print table ──
    suspicious_verdicts = {"SUSPICIOUS_SKIP_FIXES", "SUSPICIOUS_VERY_FAST", "DUPLICATE_TS"}

    if args.suspicious_only:
        rows = [r for r in rows if r["_verdict"] in suspicious_verdicts]
        print(f"\nShowing {len(rows)} suspicious gaps:\n")
    else:
        print(f"\nAll {len(rows)} gaps:\n")

    header = (f"{'#':>4} {'MMSI':<12} {'Gap':>12} {'Time':>8} "
              f"{'Needed':>8} {'MaxSOG':>7} {'Ratio':>6} "
              f"{'SkipDist':>12} {'SkipNeed':>8} {'Verdict':<25}")
    print(header)
    print("-" * len(header))

    for i, r in enumerate(rows):
        dist = r["_dist_km"] or 0
        time_s = r["_time_s"] or 0
        needed = r["_needed_kn"]
        max_sog = r["_max_sog"] or 0
        ratio = r["_speed_ratio"]
        skip_dist = r["_skip_dist_km"]
        skip_need = r["_skip_needed_kn"]
        verdict = r["_verdict"]

        # Time formatting
        if time_s < 60:
            time_str = f"{time_s:.0f}s"
        elif time_s < 3600:
            time_str = f"{time_s/60:.1f}m"
        else:
            time_str = f"{time_s/3600:.2f}h"

        flag = " <<<" if verdict in suspicious_verdicts else ""

        print(
            f"{i+1:>4} {r['MMSI']:<12} {fmt_dist(dist):>12} {time_str:>8} "
            f"{needed or 0:>7.1f}kn {max_sog:>6.1f}kn {ratio or 0:>5.1f}x "
            f"{fmt_dist(skip_dist):>12} {skip_need or 0:>7.1f}kn "
            f"{verdict:<25}{flag}"
        )

    # ── Show SUSPICIOUS details ──
    suspicious = [r for r in rows if r["_verdict"] in suspicious_verdicts]
    if suspicious:
        print(f"\n{'='*100}")
        print(f"SUSPICIOUS GAPS DETAIL ({len(suspicious)} found)")
        print(f"{'='*100}")
        for r in suspicious:
            dist = r["_dist_km"] or 0
            time_s = r["_time_s"] or 0
            skip_dist = r["_skip_dist_km"]
            print(f"\n  MMSI {r['MMSI']}  verdict={r['_verdict']}")
            print(f"    Point BEFORE: lat={r['_prev_lat']:.5f}  lon={r['_prev_lon']:.5f}  SOG={r['_prev_sog']:.1f}")
            print(f"    Point THIS:   lat={r['Latitude']:.5f}  lon={r['Longitude']:.5f}  SOG={r['SOG']:.1f}  ts={r['# Timestamp']}")
            print(
                f"    Point AFTER:  lat={r['_next_lat']:.5f}  lon={r['_next_lon']:.5f}  SOG={r['_next_sog']:.1f}"
                if r["_next_lat"] is not None
                else "    Point AFTER:  N/A"
            )
            print(f"    Gap:      {fmt_dist(dist)}  in {time_s:.0f}s  needed={r['_needed_kn'] or 0:.1f}kn  vs maxSOG={r['_max_sog'] or 0:.1f}kn")
            if skip_dist is not None:
                print(f"    Skip gap: {fmt_dist(skip_dist)}  needed={r['_skip_needed_kn'] or 0:.1f}kn  (removing THIS point)")
            print(f"    Dist to next: {fmt_dist(r['_dist_next_km'])}  in {r['_time_next_s'] or 0:.0f}s")

    top_gaps.unpersist()
    print("\nDone.")
    spark.stop()


if __name__ == "__main__":
    main()
