import argparse
import os
import time
from dotenv import load_dotenv
import psycopg


SQL_POINTS = """
WITH env AS (
  SELECT ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326) AS e
)
SELECT
  COUNT(*) AS point_count,
  COUNT(DISTINCT mmsi) AS vessel_count
FROM ais_points_cleaned, env
WHERE ts >= %(t0)s
  AND ts <  %(t1)s
  AND geom && env.e
  AND ST_Within(geom, env.e);
"""

SQL_MMSI_LIST = """
WITH env AS (
  SELECT ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326) AS e
)
SELECT DISTINCT mmsi
FROM ais_points_cleaned, env
WHERE ts >= %(t0)s
  AND ts <  %(t1)s
  AND geom && env.e
  AND ST_Within(geom, env.e)
ORDER BY mmsi
LIMIT %(limit)s;
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--t0", required=True, help="Start time, e.g. 2026-02-22T10:00:00Z")
    p.add_argument("--t1", required=True, help="End time, e.g. 2026-02-22T11:00:00Z")
    p.add_argument("--min-lon", type=float, required=True)
    p.add_argument("--min-lat", type=float, required=True)
    p.add_argument("--max-lon", type=float, required=True)
    p.add_argument("--max-lat", type=float, required=True)
    p.add_argument("--list-mmsi", action="store_true", help="Also print a sample list of MMSI")
    p.add_argument("--list-limit", type=int, default=25)
    p.add_argument("--explain", action="store_true", help="Run EXPLAIN ANALYZE instead of the query")
    args = p.parse_args()

    load_dotenv()
    db_url = os.environ["DATABASE_URL"]

    params = {
        "t0": args.t0,
        "t1": args.t1,
        "min_lon": args.min_lon,
        "min_lat": args.min_lat,
        "max_lon": args.max_lon,
        "max_lat": args.max_lat,
        "limit": args.list_limit,
    }

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC';")

            if args.explain:
                q = "EXPLAIN (ANALYZE, BUFFERS) " + SQL_POINTS
                cur.execute(q, params)
                print("\n".join(r[0] for r in cur.fetchall()))
                return

            t0 = time.perf_counter()
            cur.execute(SQL_POINTS, params)
            point_count, vessel_count = cur.fetchone()
            ms = (time.perf_counter() - t0) * 1000.0

            print(f"points={point_count:,} vessels={vessel_count:,} time_ms={ms:.2f}")

            if args.list_mmsi:
                cur.execute(SQL_MMSI_LIST, params)
                mmsi = [r[0] for r in cur.fetchall()]
                print(f"sample_mmsi({len(mmsi)}): {mmsi}")


if __name__ == "__main__":
    main()