"""
Standalone validation script – runs the full pipeline UP TO the outlier
detector, then runs the outlier detector, and compares before vs after.
This isolates ONLY the rows removed by ais_pipeline.steps.remove_outliers.

Reports:
  - Per-MMSI: rows deleted, max consecutive gap BEFORE and AFTER cleaning
  - Deep dive on top affected ships: shows sample deleted rows with SOG,
    distance to prev/next, expected distance, and why the detector
    flagged them

Usage:
    python -m ais_pipeline.tools.validate_cleaning
    python -m ais_pipeline.tools.validate_cleaning --input AISDATA/other.csv --top 50 --deep 5
"""

import argparse
import sys
from pathlib import Path

# Add repo root to path so this script can be run both directly and as a module.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ais_pipeline.steps import remove_duplicates
from ais_pipeline.steps import remove_outliers
from ais_pipeline.steps import remove_shiptypes
from ais_pipeline.steps import ship_type
from ais_pipeline.steps import trim_stationary

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


def build_spark():
    configure_java_environment(REPO_ROOT, verbose=False)
    configure_hadoop_environment(REPO_ROOT, verbose=False)
    configure_pyspark_python()
    configure_spark_environment(REPO_ROOT)

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("validate_outlier_detection")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def run_pipeline_before_outliers(spark, input_path):
    """Run every pipeline step from main.py BEFORE remove_outliers."""
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(str(input_path))
    )

    df = remove_duplicates.deduplicate_and_filter(df)

    timestamp_col = "# Timestamp"
    timestamp_expr = F.coalesce(
        F.try_to_timestamp(F.col(timestamp_col), F.lit("dd/MM/yyyy HH:mm:ss")),
        F.try_to_timestamp(F.col(timestamp_col), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
        F.try_to_timestamp(F.col(timestamp_col)),
    )
    df = (
        df.withColumn(timestamp_col, timestamp_expr)
        .withColumn("Latitude", F.col("Latitude").cast("double"))
        .withColumn("Longitude", F.col("Longitude").cast("double"))
        .withColumn("SOG", F.col("SOG").cast("double"))
        .withColumn("COG", F.col("COG").cast("double"))
        .filter(F.col(timestamp_col).isNotNull())
        .filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull())
        .filter(F.col("SOG").isNotNull())
    )

    df = trim_stationary.trim_stationary(df)
    df = ship_type.fill_ship_type(df)
    df = ship_type.remove_undefined_ship_type(df)
    df = remove_shiptypes.remove_shiptypes(df)

    return df


def compute_max_consecutive_gap(df):
    """Max haversine distance between consecutive points per MMSI."""
    w = Window.partitionBy("MMSI").orderBy("# Timestamp")
    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)

    with_dist = df.withColumn(
        "_consec_km",
        haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude")),
    )
    return with_dist.groupBy("MMSI").agg(
        F.max("_consec_km").alias("max_consec_gap_km")
    )


def tag_deleted_rows(before_df, after_df):
    """
    Return the before_df with extra columns:
      _kept       : True if the row survived outlier detection
      _dist_prev  : haversine km to previous row (in original order)
      _dist_next  : haversine km to next row (in original order)
      _time_s_prev: seconds since previous row
      _prev_sog   : SOG of the previous row
      _exp_km     : expected km from outlier distance formula
      _margin     : margin used by outlier distance formula
    """
    after_keys = (
        after_df.select("MMSI", "# Timestamp")
        .withColumn("_kept", F.lit(True))
    )

    tagged = before_df.join(after_keys, on=["MMSI", "# Timestamp"], how="left")
    tagged = tagged.withColumn("_kept", F.coalesce(F.col("_kept"), F.lit(False)))

    w = Window.partitionBy("MMSI").orderBy("# Timestamp")

    prev_lat = F.lag("Latitude").over(w)
    prev_lon = F.lag("Longitude").over(w)
    prev_sog = F.lag("SOG").over(w)
    prev_ts  = F.lag("# Timestamp").over(w)
    next_lat = F.lead("Latitude").over(w)
    next_lon = F.lead("Longitude").over(w)

    time_s = F.col("# Timestamp").cast("long") - prev_ts.cast("long")
    time_h = time_s / 3600.0
    dist_prev = haversine_km(prev_lat, prev_lon, F.col("Latitude"), F.col("Longitude"))
    dist_next = haversine_km(F.col("Latitude"), F.col("Longitude"), next_lat, next_lon)

    base_margin = 1.2
    time_scale = 0.3
    exp_km = prev_sog * KNOTS_TO_KMH * time_h
    margin = base_margin * (1.0 + time_scale * time_h)

    tagged = (
        tagged
        .withColumn("_dist_prev_km", F.round(dist_prev, 4))
        .withColumn("_dist_next_km", F.round(dist_next, 4))
        .withColumn("_time_s_prev", time_s)
        .withColumn("_prev_sog", prev_sog)
        .withColumn("_exp_km", F.round(exp_km, 4))
        .withColumn("_margin", F.round(margin, 4))
        .withColumn("_allowed_km", F.round(exp_km * margin, 4))
        .withColumn("_implied_speed_knots",
                     F.when(time_h > 0,
                            F.round(dist_prev / (time_h * KNOTS_TO_KMH), 2)))
    )
    return tagged


def main():
    parser = argparse.ArgumentParser(
        description="Validate ONLY the outlier detector step in isolation"
    )
    parser.add_argument(
        "--input",
        default=str(AISDATA_DIR / "aisdk-2026-02-05.csv"),
        help="Path to original AIS CSV file",
    )
    parser.add_argument(
        "--top", type=int, default=50,
        help="Show top N MMSIs with most deletions (default: 50)",
    )
    parser.add_argument(
        "--deep", type=int, default=5,
        help="Deep-dive into this many of the top affected MMSIs (default: 5)",
    )
    parser.add_argument(
        "--sample-rows", type=int, default=15,
        help="Number of deleted rows to show per deep-dive ship (default: 15)",
    )
    parser.add_argument(
        "--report", choices=["all", "deleted", "gaps"], default="all",
        help="Which report: 'deleted' = top MMSIs by rows deleted, "
             "'gaps' = top MMSIs where outlier detector reduced MaxGap, "
             "'all' = both (default: all)",
    )
    args = parser.parse_args()

    spark = build_spark()
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(CHECKPOINT_DIR))

    # ── Run pipeline up to (but not including) outlier removal ──
    print("Running pipeline steps before outlier detection...")
    before_outliers = run_pipeline_before_outliers(spark, args.input)
    before_outliers.cache()
    count_before = before_outliers.count()
    print(f"  Rows entering outlier detector: {count_before:,}")

    # ── Run the outlier detector ──
    print("Running outlier detector...")
    after_outliers = remove_outliers.remove_gps_outliers(before_outliers)
    after_outliers.cache()
    count_after = after_outliers.count()
    total_deleted = count_before - count_after
    print(f"  Rows after outlier detector:    {count_after:,}")
    print(f"  Rows removed by outlier detector: {total_deleted:,}")

    # ── Per-MMSI row counts ──
    before_counts = before_outliers.groupBy("MMSI").agg(
        F.count("*").alias("before_rows")
    )
    after_counts = after_outliers.groupBy("MMSI").agg(
        F.count("*").alias("after_rows")
    )

    comparison = before_counts.join(after_counts, on="MMSI", how="left")
    comparison = comparison.withColumn(
        "after_rows", F.coalesce(F.col("after_rows"), F.lit(0))
    ).withColumn(
        "deleted_rows", F.col("before_rows") - F.col("after_rows")
    ).withColumn(
        "pct_deleted",
        F.round(
            (F.col("before_rows") - F.col("after_rows"))
            / F.col("before_rows") * 100, 1
        ),
    )

    affected = comparison.filter(F.col("deleted_rows") > 0)
    affected_count = affected.count()

    # ── Max consecutive gap BEFORE and AFTER per MMSI ──
    print("Computing max consecutive gaps before and after cleaning...")
    gaps_before = compute_max_consecutive_gap(before_outliers).withColumnRenamed(
        "max_consec_gap_km", "gap_before_km"
    )
    gaps_after = compute_max_consecutive_gap(after_outliers).withColumnRenamed(
        "max_consec_gap_km", "gap_after_km"
    )

    report = (
        affected
        .join(gaps_before, on="MMSI", how="left")
        .join(gaps_after,  on="MMSI", how="left")
        .withColumn("gap_before_km",
                     F.round(F.coalesce(F.col("gap_before_km"), F.lit(0.0)), 3))
        .withColumn("gap_after_km",
                     F.round(F.coalesce(F.col("gap_after_km"), F.lit(0.0)), 3))
        .orderBy(F.col("deleted_rows").desc())
    )

    # ══════════════════════════════════════════════════════════════
    #  SUMMARY TABLE
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 100)
    print("OUTLIER DETECTOR VALIDATION REPORT")
    print("=" * 100)
    print(f"Rows before outlier detector: {count_before:>12,}")
    print(f"Rows after outlier detector:  {count_after:>12,}")
    print(f"Rows removed:                 {total_deleted:>12,}")
    print(f"MMSIs affected:               {affected_count:>12,}")
    print("=" * 100)

    # ══════════════════════════════════════════════════════════════
    #  TOP N BY MOST DELETED ROWS
    # ══════════════════════════════════════════════════════════════
    if args.report in ("all", "deleted"):
        header = (f"{'MMSI':<12} {'Before':>8} {'After':>8} {'Deleted':>8} "
                  f"{'%Del':>6} {'MaxGap Before':>14} {'MaxGap After':>14}")
        print(f"\nTop {args.top} MMSIs by rows deleted:\n")
        print(header)
        print("-" * len(header))

        top_rows = report.limit(args.top).collect()
        for r in top_rows:
            print(
                f"{r['MMSI']:<12} {r['before_rows']:>8,} {r['after_rows']:>8,} "
                f"{r['deleted_rows']:>8,} {r['pct_deleted']:>5.1f}% "
                f"{r['gap_before_km']:>13.3f}  {r['gap_after_km']:>13.3f}"
            )

    # ══════════════════════════════════════════════════════════════
    #  TOP N WHERE OUTLIER DETECTOR REDUCED MAX-GAP
    # ══════════════════════════════════════════════════════════════
    def fmt_dist(km):
        """Format distance: metres if < 1 km, else km."""
        if km is None:
            return "     N/A"
        if km < 1.0:
            return f"{km * 1000:>7.1f} m"
        return f"{km:>7.3f} km"

    if args.report in ("all", "gaps"):
      # Only MMSIs where the outlier detector actually reduced the max gap
      fixed_gap_report = (
        before_counts
        .join(after_counts, on="MMSI", how="left")
        .withColumn("after_rows", F.coalesce(F.col("after_rows"), F.lit(0)))
        .withColumn("deleted_rows", F.col("before_rows") - F.col("after_rows"))
        .join(gaps_before, on="MMSI", how="left")
        .join(gaps_after,  on="MMSI", how="left")
        .withColumn("gap_before_km",
                     F.coalesce(F.col("gap_before_km"), F.lit(0.0)))
        .withColumn("gap_after_km",
                     F.coalesce(F.col("gap_after_km"), F.lit(0.0)))
        .withColumn("gap_reduction_km",
                     F.col("gap_before_km") - F.col("gap_after_km"))
        .withColumn("gap_reduction_pct",
                     F.when(F.col("gap_before_km") > 0,
                            F.round((F.col("gap_before_km") - F.col("gap_after_km"))
                                    / F.col("gap_before_km") * 100, 1))
                     .otherwise(F.lit(0.0)))
          .filter(F.col("gap_reduction_km") > 0.001)
          .orderBy(F.col("gap_before_km").desc())
      )

      gap_rows = fixed_gap_report.limit(args.top).collect()

      gap_header = (f"{'MMSI':<12} {'Del':>6} {'MaxGap Before':>14} {'MaxGap After':>14} "
                    f"{'Reduction':>14} {'Red%':>6}")
      print(f"\n\nTop {args.top} MMSIs where outlier detector REDUCED the MaxGap:\n")
      print(gap_header)
      print("-" * len(gap_header))
      for r in gap_rows:
          gb = r["gap_before_km"]
          ga = r["gap_after_km"]
          rd = r["gap_reduction_km"]
          rp = r["gap_reduction_pct"]
          print(
              f"{r['MMSI']:<12} {r['deleted_rows']:>6,} "
              f"{fmt_dist(gb):>14} {fmt_dist(ga):>14} "
              f"{fmt_dist(rd):>14} {rp:>5.1f}%"
          )

    # ══════════════════════════════════════════════════════════════
    #  DEEP DIVE: show WHY rows were deleted for top ships
    # ══════════════════════════════════════════════════════════════
    if args.deep > 0 and args.report in ("all", "deleted"):
        print("\n\nTagging deleted rows with diagnostic info...")
        tagged = tag_deleted_rows(before_outliers, after_outliers)
        tagged.cache()

        # Global stats on deleted rows
        deleted_all = tagged.filter(~F.col("_kept") & F.col("_prev_sog").isNotNull())

        stats = deleted_all.agg(
            F.count("*").alias("n"),
            F.round(F.mean("_prev_sog"), 2).alias("avg_prev_sog"),
            F.round(F.expr("percentile_approx(_prev_sog, 0.5)"), 2).alias("median_prev_sog"),
            F.round(F.mean("_dist_prev_km"), 4).alias("avg_dist_prev_km"),
            F.round(F.mean("_allowed_km"), 4).alias("avg_allowed_km"),
            F.round(F.mean("_implied_speed_knots"), 2).alias("avg_implied_speed"),
            F.sum(F.when(F.col("_prev_sog") < 1.0, 1).otherwise(0)).alias("prev_sog_lt_1"),
            F.sum(F.when(F.col("_prev_sog") < 0.5, 1).otherwise(0)).alias("prev_sog_lt_05"),
            F.sum(F.when(F.col("_implied_speed_knots") > 50, 1).otherwise(0)).alias("implied_gt_50kn"),
        ).collect()[0]

        print("\n" + "=" * 100)
        print("GLOBAL STATS ON DELETED ROWS")
        print("=" * 100)
        print(f"  Total deleted rows (with prev):   {stats['n']:,}")
        print(f"  Avg prev_sog of deleted rows:     {stats['avg_prev_sog']} knots")
        print(f"  Median prev_sog of deleted rows:  {stats['median_prev_sog']} knots")
        print(f"  Avg distance to prev:             {stats['avg_dist_prev_km']} km")
        print(f"  Avg allowed distance (exp*margin):{stats['avg_allowed_km']} km")
        print(f"  Avg implied speed:                {stats['avg_implied_speed']} knots")
        print(f"  Deleted where prev_sog < 1.0 kn:  {stats['prev_sog_lt_1']:,}"
              f"  ({stats['prev_sog_lt_1']*100/max(stats['n'],1):.1f}%)")
        print(f"  Deleted where prev_sog < 0.5 kn:  {stats['prev_sog_lt_05']:,}"
              f"  ({stats['prev_sog_lt_05']*100/max(stats['n'],1):.1f}%)")
        print(f"  Deleted where implied > 50 kn:    {stats['implied_gt_50kn']:,}"
              f"  ({stats['implied_gt_50kn']*100/max(stats['n'],1):.1f}%)")

        # Per-ship deep dive
        deep_mmsis = [r["MMSI"] for r in top_rows[:args.deep]]

        for mmsi in deep_mmsis:
            ship_all = (
                tagged.filter(F.col("MMSI") == mmsi)
                .orderBy("# Timestamp")
            )
            ship_deleted = ship_all.filter(~F.col("_kept"))
            n_del = ship_deleted.count()

            # SOG distribution of deleted rows
            sog_stats = ship_deleted.agg(
                F.round(F.mean("SOG"), 2).alias("avg_sog"),
                F.round(F.mean("_prev_sog"), 2).alias("avg_prev_sog"),
                F.round(F.mean("_dist_prev_km"), 4).alias("avg_dist"),
                F.round(F.mean("_allowed_km"), 4).alias("avg_allowed"),
            ).collect()[0]

            print(f"\n{'─' * 100}")
            print(f"DEEP DIVE: MMSI {mmsi}  ({n_del} rows deleted)")
            print(f"  Avg SOG of deleted rows:  {sog_stats['avg_sog']} kn")
            print(f"  Avg prev_sog:             {sog_stats['avg_prev_sog']} kn")
            print(f"  Avg dist to prev:         {sog_stats['avg_dist']} km")
            print(f"  Avg allowed dist:         {sog_stats['avg_allowed']} km")

            # Show sample deleted rows
            sample = (
                ship_deleted
                .select(
                    "# Timestamp", "Latitude", "Longitude", "SOG",
                    "_prev_sog", "_dist_prev_km", "_dist_next_km",
                    "_time_s_prev", "_exp_km", "_allowed_km",
                    "_implied_speed_knots",
                )
                .orderBy("# Timestamp")
                .limit(args.sample_rows)
                .collect()
            )

            print(f"\n  Sample deleted rows (first {args.sample_rows}):")
            print(f"  {'Timestamp':<28} {'Lat':>10} {'Lon':>10} {'SOG':>5} "
                  f"{'pSOG':>5} {'Dist':>7} {'Allow':>7} {'Impl kn':>8} {'dt(s)':>6}")
            print(f"  {'-'*92}")
            for row in sample:
                ts = str(row["# Timestamp"])[:19] if row["# Timestamp"] else "N/A"
                print(
                    f"  {ts:<28} {row['Latitude']:>10.5f} {row['Longitude']:>10.5f} "
                    f"{row['SOG'] or 0:>5.1f} "
                    f"{row['_prev_sog'] or 0:>5.1f} "
                    f"{row['_dist_prev_km'] or 0:>7.4f} "
                    f"{row['_allowed_km'] or 0:>7.4f} "
                    f"{row['_implied_speed_knots'] or 0:>8.2f} "
                    f"{row['_time_s_prev'] or 0:>6}"
                )

        tagged.unpersist()

    # ── Fully removed MMSIs ──
    fully_removed = comparison.filter(F.col("after_rows") == 0)
    fully_removed_count = fully_removed.count()
    if fully_removed_count > 0:
        print(f"\n{fully_removed_count} MMSIs had ALL rows removed by the outlier detector.")

    before_outliers.unpersist()
    after_outliers.unpersist()

    print("\nDone.")
    spark.stop()


if __name__ == "__main__":
    main()
