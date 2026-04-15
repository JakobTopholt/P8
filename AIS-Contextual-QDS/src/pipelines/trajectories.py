"""Trajectory construction from cleaned AIS points."""

from __future__ import annotations

import logging
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import execute_sql, fetch_one

LOGGER = logging.getLogger(__name__)


def run(conn: Connection[Any], config: AppConfig, *, truncate: bool = True) -> dict[str, int]:
    """Build raw trajectories according to MVP splitting rules."""
    schema = config.database.schema
    source_table = config.database.source_points_table
    ts_col = config.database.source_ts_column
    ship_type_col = config.database.source_ship_type_column

    if truncate:
        LOGGER.info("Clearing existing trajectory tables in schema '%s'.", schema)
        execute_sql(
            conn,
            (
                f"TRUNCATE TABLE {schema}.trajectory_points_raw, "
                f"{schema}.trajectories_raw RESTART IDENTITY CASCADE;"
            ),
        )

    if fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.study_region WHERE is_active = TRUE;") == 0:
        raise RuntimeError(
            f"No active study region found in {schema}.study_region. "
            "Insert one row and set is_active = TRUE before building trajectories."
        )

    insert_points_sql = f"""
WITH active_region AS (
    SELECT geom
    FROM {schema}.study_region
    WHERE is_active = TRUE
    LIMIT 1
),
ordered AS (
    SELECT
        p.id AS source_point_id,
        p.mmsi,
        p.{ts_col} AS ts,
        p.lat,
        p.lon,
        p.sog,
        p.cog,
        p.geom,
        LAG(p.{ts_col}) OVER (PARTITION BY p.mmsi ORDER BY p.{ts_col}, p.id) AS prev_ts,
        LAG(p.geom) OVER (PARTITION BY p.mmsi ORDER BY p.{ts_col}, p.id) AS prev_geom
    FROM {source_table} p
    CROSS JOIN active_region r
    WHERE p.geom IS NOT NULL
      AND p.{ts_col} >= %(window_start)s::timestamptz
      AND p.{ts_col} < %(window_end)s::timestamptz
      AND p.{ship_type_col} ILIKE %(vessel_class_pattern)s
      AND ST_Intersects(p.geom, r.geom)
),
flagged AS (
    SELECT
        *,
        CASE
            WHEN prev_ts IS NULL THEN 1
            WHEN ts - prev_ts > (%(max_gap_minutes)s * INTERVAL '1 minute') THEN 1
            WHEN EXTRACT(EPOCH FROM (ts - prev_ts)) <= 0 THEN 1
            WHEN prev_geom IS NOT NULL
                 AND ((ST_DistanceSphere(prev_geom, geom) / GREATEST(EXTRACT(EPOCH FROM (ts - prev_ts)), 1)) * 1.94384449)
                     > %(max_implied_speed_knots)s
                THEN 1
            ELSE 0
        END AS starts_new_traj
    FROM ordered
),
segmented AS (
    SELECT
        *,
        SUM(starts_new_traj) OVER (
            PARTITION BY mmsi
            ORDER BY ts, source_point_id
            ROWS UNBOUNDED PRECEDING
        ) AS segment_id
    FROM flagged
),
numbered AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY mmsi, segment_id) AS trajectory_id,
        ROW_NUMBER() OVER (PARTITION BY mmsi, segment_id ORDER BY ts, source_point_id) AS point_seq,
        COUNT(*) OVER (PARTITION BY mmsi, segment_id) AS n_points
    FROM segmented
)
INSERT INTO {schema}.trajectory_points_raw (
    trajectory_id,
    point_seq,
    mmsi,
    ts,
    lat,
    lon,
    sog,
    cog,
    nav_status,
    geom,
    source_point_id
)
SELECT
    trajectory_id,
    point_seq,
    mmsi,
    ts,
    lat,
    lon,
    sog,
    cog,
    NULL::text,
    geom,
    source_point_id
FROM numbered
WHERE n_points >= %(min_points)s;
"""

    LOGGER.info("Building trajectory points from %s.", source_table)
    execute_sql(
        conn,
        insert_points_sql,
        {
            "window_start": config.scope.window_start,
            "window_end": config.scope.window_end,
            "vessel_class_pattern": config.scope.vessel_class_pattern,
            "max_gap_minutes": config.trajectory.max_gap_minutes,
            "max_implied_speed_knots": config.trajectory.max_implied_speed_knots,
            "min_points": config.trajectory.min_points,
        },
    )

    insert_traj_sql = f"""
INSERT INTO {schema}.trajectories_raw (trajectory_id, mmsi, start_ts, end_ts, n_points, geom)
SELECT
    trajectory_id,
    mmsi,
    MIN(ts) AS start_ts,
    MAX(ts) AS end_ts,
    COUNT(*) AS n_points,
    ST_MakeLine(geom ORDER BY point_seq) AS geom
FROM {schema}.trajectory_points_raw
GROUP BY trajectory_id, mmsi;
"""

    execute_sql(conn, insert_traj_sql)

    points_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectory_points_raw;") or 0)
    traj_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectories_raw;") or 0)

    LOGGER.info("Built %s trajectories from %s points.", traj_count, points_count)
    return {"trajectory_points": points_count, "trajectories": traj_count}
