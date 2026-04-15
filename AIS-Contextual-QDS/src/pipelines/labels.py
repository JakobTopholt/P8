"""Ground-truth query label computation for raw trajectories."""

from __future__ import annotations

import logging
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import execute_sql, fetch_one

LOGGER = logging.getLogger(__name__)


def run(conn: Connection[Any], config: AppConfig, *, truncate: bool = True) -> dict[str, int]:
    """Compute zone-entry and corridor-membership labels."""
    schema = config.database.schema

    if truncate:
        execute_sql(conn, f"TRUNCATE TABLE {schema}.trajectory_query_labels;")

    zone_count = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.context_zones WHERE zone_name = ANY(%(zone_names)s);",
            {"zone_names": config.context.zone_names},
        )
        or 0
    )
    if zone_count != len(config.context.zone_names):
        raise RuntimeError(
            "Context zones are missing. "
            f"Expected {len(config.context.zone_names)} configured zones, found {zone_count}."
        )

    corridor_count = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.context_corridors "
                "WHERE corridor_name = %(corridor_name)s;"
            ),
            {"corridor_name": config.context.corridor_name},
        )
        or 0
    )
    if corridor_count != 1:
        raise RuntimeError(
            "Context corridor is missing. "
            f"Expected corridor '{config.context.corridor_name}' in {schema}.context_corridors."
        )

    label_sql = f"""
WITH first_points AS (
    SELECT trajectory_id, geom
    FROM {schema}.trajectory_points_raw
    WHERE point_seq = 1
),
segments AS (
    SELECT
        p1.trajectory_id,
        ST_MakeLine(p1.geom, p2.geom) AS segment_geom,
        p1.geom AS from_geom,
        p2.geom AS to_geom
    FROM {schema}.trajectory_points_raw p1
    JOIN {schema}.trajectory_points_raw p2
      ON p2.trajectory_id = p1.trajectory_id
     AND p2.point_seq = p1.point_seq + 1
),
zone_labels AS (
    SELECT
        t.trajectory_id,
        z.zone_name,
        CASE
            WHEN ST_Contains(z.geom, fp.geom) THEN FALSE
            WHEN EXISTS (
                SELECT 1
                FROM segments s
                WHERE s.trajectory_id = t.trajectory_id
                  AND (
                      (NOT ST_Contains(z.geom, s.from_geom) AND ST_Contains(z.geom, s.to_geom))
                      OR ST_Crosses(s.segment_geom, z.geom)
                  )
            ) THEN TRUE
            ELSE FALSE
        END AS zone_entry
    FROM {schema}.trajectories_raw t
    JOIN first_points fp ON fp.trajectory_id = t.trajectory_id
    JOIN {schema}.context_zones z ON z.zone_name = ANY(%(zone_names)s)
),
point_corridor_hits AS (
    SELECT
        p.trajectory_id,
        c.corridor_name,
        BOOL_OR(ST_Covers(c.geom, p.geom)) AS corridor_hit_by_point
    FROM {schema}.trajectory_points_raw p
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
    GROUP BY p.trajectory_id, c.corridor_name
),
segment_corridor_hits AS (
    SELECT
        s.trajectory_id,
        c.corridor_name,
        BOOL_OR(
            ST_Intersects(s.segment_geom, c.geom)
            AND ST_Length(ST_Intersection(s.segment_geom, c.geom)::geography) >= %(min_overlap_m)s
        ) AS corridor_hit_by_segment
    FROM segments s
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
    GROUP BY s.trajectory_id, c.corridor_name
)
INSERT INTO {schema}.trajectory_query_labels (
    trajectory_id,
    zone_name,
    corridor_name,
    zone_entry,
    corridor_membership
)
SELECT
    zl.trajectory_id,
    zl.zone_name,
    %(corridor_name)s,
    zl.zone_entry,
    COALESCE(pch.corridor_hit_by_point, FALSE) OR COALESCE(sch.corridor_hit_by_segment, FALSE) AS corridor_membership
FROM zone_labels zl
LEFT JOIN point_corridor_hits pch
  ON pch.trajectory_id = zl.trajectory_id
LEFT JOIN segment_corridor_hits sch
  ON sch.trajectory_id = zl.trajectory_id;
"""

    LOGGER.info("Computing trajectory-level query labels.")
    execute_sql(
        conn,
        label_sql,
        {
            "zone_names": config.context.zone_names,
            "corridor_name": config.context.corridor_name,
            "min_overlap_m": config.queries.min_corridor_overlap_meters,
        },
    )

    label_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels;") or 0)
    zone_positive = int(
        fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels WHERE zone_entry = TRUE;") or 0
    )
    corridor_positive = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels WHERE corridor_membership = TRUE;",
        )
        or 0
    )

    LOGGER.info(
        "Computed %s labels (%s zone-positive, %s corridor-positive).",
        label_count,
        zone_positive,
        corridor_positive,
    )
    return {
        "labels": label_count,
        "zone_positive": zone_positive,
        "corridor_positive": corridor_positive,
    }
