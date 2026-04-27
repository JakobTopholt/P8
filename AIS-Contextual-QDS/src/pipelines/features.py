"""Compute reusable per-point context features for AIS-QDS methods."""

from __future__ import annotations

import logging
from typing import Any

from psycopg import Connection, sql

from ..config import AppConfig
from ..db import execute_sql, fetch_one

LOGGER = logging.getLogger(__name__)


def run(conn: Connection[Any], config: AppConfig, *, truncate: bool = True) -> dict[str, int]:
    """Compute context and local-shape features for raw trajectory points."""
    schema = config.database.schema

    trajectory_points = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectory_points_raw;") or 0)
    if trajectory_points <= 0:
        raise RuntimeError("No raw trajectory points available. Run prepare-data or build-trajectories first.")

    zone_count = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.context_zones WHERE zone_name = ANY(%(zone_names)s);",
            {"zone_names": config.context.zone_names},
        )
        or 0
    )
    if zone_count != len(config.context.zone_names):
        raise RuntimeError("Context zones are missing or incomplete. Run load-context first.")

    corridor_count = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.context_corridors WHERE corridor_name = %(corridor_name)s;",
            {"corridor_name": config.context.corridor_name},
        )
        or 0
    )
    if corridor_count != 1:
        raise RuntimeError("Context corridor is missing. Run load-context first.")

    if truncate:
        execute_sql(conn, f"TRUNCATE TABLE {schema}.trajectory_point_context_features;")

    insert_sql = f"""
WITH base AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        p.geom,
        LAG(p.geom) OVER (PARTITION BY p.trajectory_id ORDER BY p.point_seq) AS prev_geom,
        LEAD(p.geom) OVER (PARTITION BY p.trajectory_id ORDER BY p.point_seq) AS next_geom
    FROM {schema}.trajectory_points_raw p
),
point_context AS (
    SELECT
        b.trajectory_id,
        b.point_seq,
        inside_zone.zone_name AS inside_zone_name,
        nearest_zone.zone_name AS nearest_zone_name,
        COALESCE(ST_Covers(c.geom, b.geom), FALSE) AS inside_corridor,
        nearest_zone.distance_m AS distance_to_nearest_zone_boundary_m,
        ST_Distance(b.geom::geography, ST_Boundary(c.geom)::geography) AS distance_to_corridor_boundary_m,
        CASE
            WHEN b.prev_geom IS NULL OR b.next_geom IS NULL THEN NULL::double precision
            ELSE DEGREES(
                ABS(
                    ATAN2(
                        SIN(ST_Azimuth(b.geom, b.next_geom) - ST_Azimuth(b.prev_geom, b.geom)),
                        COS(ST_Azimuth(b.geom, b.next_geom) - ST_Azimuth(b.prev_geom, b.geom))
                    )
                )
            )
        END AS local_turn_degrees,
        CASE
            WHEN b.prev_geom IS NULL OR b.next_geom IS NULL OR ST_Equals(b.prev_geom, b.next_geom) THEN NULL::double precision
            ELSE ST_Distance(b.geom::geography, ST_MakeLine(b.prev_geom, b.next_geom)::geography)
        END AS local_deviation_m
    FROM base b
    LEFT JOIN LATERAL (
        SELECT z.zone_name
        FROM {schema}.context_zones z
        WHERE z.zone_name = ANY(%(zone_names)s)
          AND ST_Covers(z.geom, b.geom)
        ORDER BY z.zone_name
        LIMIT 1
    ) inside_zone ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            z.zone_name,
            ST_Distance(b.geom::geography, ST_Boundary(z.geom)::geography) AS distance_m
        FROM {schema}.context_zones z
        WHERE z.zone_name = ANY(%(zone_names)s)
        ORDER BY b.geom <-> z.geom
        LIMIT 1
    ) nearest_zone ON TRUE
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
),
state_history AS (
    SELECT
        *,
        LAG(point_seq) OVER (PARTITION BY trajectory_id ORDER BY point_seq) AS prev_point_seq,
        LAG(inside_zone_name) OVER (PARTITION BY trajectory_id ORDER BY point_seq) AS prev_inside_zone_name,
        LAG(inside_corridor) OVER (PARTITION BY trajectory_id ORDER BY point_seq) AS prev_inside_corridor
    FROM point_context
),
with_transitions AS (
    SELECT
        *,
        CASE
            WHEN prev_point_seq IS NULL THEN FALSE
            ELSE inside_zone_name IS DISTINCT FROM prev_inside_zone_name
        END AS zone_transition,
        CASE
            WHEN prev_point_seq IS NULL THEN FALSE
            ELSE inside_corridor IS DISTINCT FROM prev_inside_corridor
        END AS corridor_transition
    FROM state_history
)
INSERT INTO {schema}.trajectory_point_context_features (
    trajectory_id,
    point_seq,
    inside_zone_name,
    nearest_zone_name,
    inside_corridor,
    distance_to_nearest_zone_boundary_m,
    distance_to_corridor_boundary_m,
    zone_transition,
    corridor_transition,
    local_turn_degrees,
    local_deviation_m
)
SELECT
    trajectory_id,
    point_seq,
    inside_zone_name,
    nearest_zone_name,
    inside_corridor,
    distance_to_nearest_zone_boundary_m,
    distance_to_corridor_boundary_m,
    zone_transition,
    corridor_transition,
    local_turn_degrees,
    local_deviation_m
FROM with_transitions
ON CONFLICT (trajectory_id, point_seq)
DO UPDATE SET
    inside_zone_name = EXCLUDED.inside_zone_name,
    nearest_zone_name = EXCLUDED.nearest_zone_name,
    inside_corridor = EXCLUDED.inside_corridor,
    distance_to_nearest_zone_boundary_m = EXCLUDED.distance_to_nearest_zone_boundary_m,
    distance_to_corridor_boundary_m = EXCLUDED.distance_to_corridor_boundary_m,
    zone_transition = EXCLUDED.zone_transition,
    corridor_transition = EXCLUDED.corridor_transition,
    local_turn_degrees = EXCLUDED.local_turn_degrees,
    local_deviation_m = EXCLUDED.local_deviation_m,
    computed_at = NOW();
"""

    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL work_mem = {};").format(sql.Literal("128MB")))
        cur.execute(
            insert_sql,
            {
                "zone_names": config.context.zone_names,
                "corridor_name": config.context.corridor_name,
            },
        )

    execute_sql(conn, f"ANALYZE {schema}.trajectory_point_context_features;")

    feature_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectory_point_context_features;") or 0)
    zone_transitions = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.trajectory_point_context_features WHERE zone_transition;",
        )
        or 0
    )
    corridor_transitions = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.trajectory_point_context_features WHERE corridor_transition;",
        )
        or 0
    )

    LOGGER.info(
        "Computed %s point features (%s zone transitions, %s corridor transitions).",
        feature_count,
        zone_transitions,
        corridor_transitions,
    )
    return {
        "point_features": feature_count,
        "zone_transitions": zone_transitions,
        "corridor_transitions": corridor_transitions,
    }
