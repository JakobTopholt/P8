"""Shared query-semantics helpers for optimized and segment-exact modes."""

from __future__ import annotations

QUERY_MODES = {"optimized", "segment_exact"}


def normalize_query_mode(mode: str | None, *, default: str) -> str:
    """Normalize and validate query mode selection."""
    raw_value = default if mode is None else mode
    normalized = raw_value.strip().lower()
    if normalized not in QUERY_MODES:
        raise ValueError(f"query mode must be one of {sorted(QUERY_MODES)}, got {raw_value!r}")
    return normalized


def zone_entry_optimized(
    *,
    starts_inside: bool,
    has_point_inside: bool,
    line_crosses_boundary: bool,
) -> bool:
    """Optimized zone-entry semantics used for the default workflow."""
    if starts_inside:
        return False
    return has_point_inside or line_crosses_boundary


def zone_entry_segment_exact(
    *,
    starts_inside: bool,
    segment_enters_zone: bool,
    line_crosses_boundary: bool,
) -> bool:
    """Segment-level zone-entry semantics used for audit/truth runs."""
    if starts_inside:
        return False
    return segment_enters_zone or line_crosses_boundary


def corridor_membership_optimized(
    *,
    has_point_inside: bool,
    overlap_meters: float,
    min_overlap_meters: float,
) -> bool:
    """Optimized corridor semantics using point hits plus full-line overlap."""
    if has_point_inside:
        return True
    return overlap_meters >= min_overlap_meters


def corridor_membership_segment_exact(
    *,
    point_covered: bool,
    segment_overlap_meters: float,
    min_overlap_meters: float,
) -> bool:
    """Segment-level corridor semantics using covered points or one qualifying segment."""
    if point_covered:
        return True
    return segment_overlap_meters >= min_overlap_meters


def build_raw_label_insert_sql(schema: str, *, mode: str) -> str:
    """Build INSERT SQL for trajectory_query_labels under the selected mode."""
    normalized_mode = normalize_query_mode(mode, default="optimized")

    if normalized_mode == "segment_exact":
        return f"""
WITH first_points AS (
    SELECT trajectory_id, geom
    FROM {schema}.trajectory_points_raw
    WHERE point_seq = 1
),
zones AS (
    SELECT zone_name, geom
    FROM {schema}.context_zones
    WHERE zone_name = ANY(%(zone_names)s)
),
corridor AS (
    SELECT corridor_name, geom
    FROM {schema}.context_corridors
    WHERE corridor_name = %(corridor_name)s
),
segments AS MATERIALIZED (
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
zone_segment_hits AS (
    SELECT
        s.trajectory_id,
        z.zone_name,
        BOOL_OR(
            (NOT ST_Contains(z.geom, s.from_geom) AND ST_Contains(z.geom, s.to_geom))
            OR ST_Crosses(s.segment_geom, z.geom)
        ) AS segment_enters_zone
    FROM segments s
    JOIN zones z ON ST_Intersects(s.segment_geom, z.geom)
    GROUP BY s.trajectory_id, z.zone_name
),
zone_labels AS (
    SELECT
        t.trajectory_id,
        z.zone_name,
        CASE
            WHEN ST_Contains(z.geom, fp.geom) THEN FALSE
            WHEN COALESCE(zsh.segment_enters_zone, FALSE) THEN TRUE
            ELSE FALSE
        END AS zone_entry
    FROM {schema}.trajectories_raw t
    JOIN first_points fp ON fp.trajectory_id = t.trajectory_id
    JOIN zones z ON TRUE
    LEFT JOIN zone_segment_hits zsh
      ON zsh.trajectory_id = t.trajectory_id
     AND zsh.zone_name = z.zone_name
),
point_corridor_hits AS (
    SELECT
        p.trajectory_id,
        c.corridor_name,
        BOOL_OR(ST_Covers(c.geom, p.geom)) AS corridor_hit_by_point
    FROM {schema}.trajectory_points_raw p
    JOIN corridor c ON ST_Intersects(c.geom, p.geom)
    GROUP BY p.trajectory_id, c.corridor_name
),
segment_corridor_hits AS (
    SELECT
        s.trajectory_id,
        c.corridor_name,
        BOOL_OR(
            ST_Length(ST_Intersection(s.segment_geom, c.geom)::geography) >= %(min_overlap_m)s
        ) AS corridor_hit_by_segment
    FROM segments s
    JOIN corridor c ON ST_Intersects(s.segment_geom, c.geom)
    GROUP BY s.trajectory_id, c.corridor_name
)
INSERT INTO {schema}.trajectory_query_labels (
    trajectory_id,
    zone_name,
    corridor_name,
    label_mode,
    zone_entry,
    corridor_membership
)
SELECT
    zl.trajectory_id,
    zl.zone_name,
    %(corridor_name)s,
    %(label_mode)s,
    zl.zone_entry,
    COALESCE(pch.corridor_hit_by_point, FALSE) OR COALESCE(sch.corridor_hit_by_segment, FALSE) AS corridor_membership
FROM zone_labels zl
LEFT JOIN point_corridor_hits pch
  ON pch.trajectory_id = zl.trajectory_id
LEFT JOIN segment_corridor_hits sch
  ON sch.trajectory_id = zl.trajectory_id;
"""

    return f"""
WITH zone_point_hits AS (
    SELECT
        p.trajectory_id,
        z.zone_name,
        BOOL_OR(ST_Contains(z.geom, p.geom)) AS point_inside_zone
    FROM {schema}.trajectory_points_raw p
    JOIN {schema}.context_zones z
      ON z.zone_name = ANY(%(zone_names)s)
     AND ST_Intersects(z.geom, p.geom)
    GROUP BY p.trajectory_id, z.zone_name
),
zone_labels AS (
    SELECT
        t.trajectory_id,
        z.zone_name,
        CASE
            WHEN ST_Contains(z.geom, ST_StartPoint(t.geom)) THEN FALSE
            WHEN COALESCE(zph.point_inside_zone, FALSE) OR ST_Crosses(t.geom, z.geom) THEN TRUE
            ELSE FALSE
        END AS zone_entry
    FROM {schema}.trajectories_raw t
    JOIN {schema}.context_zones z ON z.zone_name = ANY(%(zone_names)s)
    LEFT JOIN zone_point_hits zph
      ON zph.trajectory_id = t.trajectory_id
     AND zph.zone_name = z.zone_name
),
corridor_point_hits AS (
    SELECT
        p.trajectory_id,
        c.corridor_name,
        BOOL_OR(ST_Contains(c.geom, p.geom)) AS point_inside_corridor
    FROM {schema}.trajectory_points_raw p
    JOIN {schema}.context_corridors c
      ON c.corridor_name = %(corridor_name)s
     AND ST_Intersects(c.geom, p.geom)
    GROUP BY p.trajectory_id, c.corridor_name
),
corridor_labels AS (
    SELECT
        t.trajectory_id,
        c.corridor_name,
        CASE
            WHEN COALESCE(cph.point_inside_corridor, FALSE) THEN TRUE
            WHEN ST_Intersects(t.geom, c.geom)
                 AND ST_Length(ST_Intersection(t.geom, c.geom)::geography) >= %(min_overlap_m)s THEN TRUE
            ELSE FALSE
        END AS corridor_membership
    FROM {schema}.trajectories_raw t
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
    LEFT JOIN corridor_point_hits cph
      ON cph.trajectory_id = t.trajectory_id
     AND cph.corridor_name = c.corridor_name
)
INSERT INTO {schema}.trajectory_query_labels (
    trajectory_id,
    zone_name,
    corridor_name,
    label_mode,
    zone_entry,
    corridor_membership
)
SELECT
    zl.trajectory_id,
    zl.zone_name,
    cl.corridor_name,
    %(label_mode)s,
    zl.zone_entry,
    cl.corridor_membership
FROM zone_labels zl
JOIN corridor_labels cl
  ON cl.trajectory_id = zl.trajectory_id;
"""


def build_run_prediction_ctes_sql(
    schema: str,
    *,
    mode: str,
    run_points_where_sql: str,
    run_segments_where_sql: str | None = None,
) -> str:
    """Build CTE chain producing `preds` for simplified trajectories."""
    normalized_mode = normalize_query_mode(mode, default="optimized")
    segment_filter_sql = run_segments_where_sql or run_points_where_sql

    if normalized_mode == "segment_exact":
        return f"""
run_points AS (
    SELECT trajectory_id, point_seq, geom
    FROM {schema}.trajectories_simplified_points
    {run_points_where_sql}
),
zones AS (
    SELECT zone_name, geom
    FROM {schema}.context_zones
    WHERE zone_name = ANY(%(zone_names)s)
),
corridor AS (
    SELECT corridor_name, geom
    FROM {schema}.context_corridors
    WHERE corridor_name = %(corridor_name)s
),
run_lines AS (
    SELECT
        trajectory_id,
        ST_MakeLine(geom ORDER BY point_seq) AS geom
    FROM run_points
    GROUP BY trajectory_id
),
first_points AS (
    SELECT trajectory_id, geom
    FROM run_points
    WHERE point_seq = 1
),
cached_segments AS (
    SELECT
        trajectory_id,
        segment_seq,
        geom AS segment_geom,
        from_geom,
        to_geom
    FROM {schema}.trajectories_simplified_segments
    {segment_filter_sql}
),
cached_segment_presence AS (
    SELECT EXISTS (SELECT 1 FROM cached_segments) AS has_cached_segments
),
segments AS MATERIALIZED (
    SELECT
        trajectory_id,
        segment_seq,
        segment_geom,
        from_geom,
        to_geom
    FROM cached_segments
    UNION ALL
    SELECT
        p1.trajectory_id,
        p1.point_seq AS segment_seq,
        ST_MakeLine(p1.geom, p2.geom) AS segment_geom,
        p1.geom AS from_geom,
        p2.geom AS to_geom
    FROM run_points p1
    JOIN run_points p2
      ON p2.trajectory_id = p1.trajectory_id
     AND p2.point_seq = p1.point_seq + 1
    WHERE NOT (SELECT has_cached_segments FROM cached_segment_presence)
),
zone_segment_hits AS (
    SELECT
        s.trajectory_id,
        z.zone_name,
        BOOL_OR(
            (NOT ST_Contains(z.geom, s.from_geom) AND ST_Contains(z.geom, s.to_geom))
            OR ST_Crosses(s.segment_geom, z.geom)
        ) AS segment_enters_zone
    FROM segments s
    JOIN zones z ON ST_Intersects(s.segment_geom, z.geom)
    GROUP BY s.trajectory_id, z.zone_name
),
zone_preds AS (
    SELECT
        rl.trajectory_id,
        z.zone_name,
        CASE
            WHEN ST_Contains(z.geom, fp.geom) THEN FALSE
            WHEN COALESCE(zsh.segment_enters_zone, FALSE) THEN TRUE
            ELSE FALSE
        END AS zone_entry_pred
    FROM run_lines rl
    JOIN first_points fp ON fp.trajectory_id = rl.trajectory_id
    JOIN zones z ON TRUE
    LEFT JOIN zone_segment_hits zsh
      ON zsh.trajectory_id = rl.trajectory_id
     AND zsh.zone_name = z.zone_name
),
point_corridor_hits AS (
    SELECT
        p.trajectory_id,
        c.corridor_name,
        BOOL_OR(ST_Covers(c.geom, p.geom)) AS corridor_hit_by_point
    FROM run_points p
    JOIN corridor c ON ST_Intersects(c.geom, p.geom)
    GROUP BY p.trajectory_id, c.corridor_name
),
segment_corridor_hits AS (
    SELECT
        s.trajectory_id,
        c.corridor_name,
        BOOL_OR(
            ST_Length(ST_Intersection(s.segment_geom, c.geom)::geography) >= %(min_overlap_m)s
        ) AS corridor_hit_by_segment
    FROM segments s
    JOIN corridor c ON ST_Intersects(s.segment_geom, c.geom)
    GROUP BY s.trajectory_id, c.corridor_name
),
corridor_preds AS (
    SELECT
        rl.trajectory_id,
        c.corridor_name,
        COALESCE(pch.corridor_hit_by_point, FALSE) OR COALESCE(sch.corridor_hit_by_segment, FALSE) AS corridor_pred
    FROM run_lines rl
    JOIN corridor c ON TRUE
    LEFT JOIN point_corridor_hits pch
      ON pch.trajectory_id = rl.trajectory_id
     AND pch.corridor_name = c.corridor_name
    LEFT JOIN segment_corridor_hits sch
      ON sch.trajectory_id = rl.trajectory_id
     AND sch.corridor_name = c.corridor_name
),
preds AS (
    SELECT
        zp.trajectory_id,
        zp.zone_name,
        cp.corridor_name,
        zp.zone_entry_pred,
        cp.corridor_pred
    FROM zone_preds zp
    JOIN corridor_preds cp
      ON cp.trajectory_id = zp.trajectory_id
)
"""

    return f"""
run_points AS (
    SELECT trajectory_id, point_seq, geom
    FROM {schema}.trajectories_simplified_points
    {run_points_where_sql}
),
run_lines AS (
    SELECT
        trajectory_id,
        ST_MakeLine(geom ORDER BY point_seq) AS geom
    FROM run_points
    GROUP BY trajectory_id
),
zone_point_hits AS (
    SELECT
        p.trajectory_id,
        z.zone_name,
        BOOL_OR(ST_Contains(z.geom, p.geom)) AS point_inside_zone
    FROM run_points p
    JOIN {schema}.context_zones z
      ON z.zone_name = ANY(%(zone_names)s)
     AND ST_Intersects(z.geom, p.geom)
    GROUP BY p.trajectory_id, z.zone_name
),
zone_preds AS (
    SELECT
        rl.trajectory_id,
        z.zone_name,
        CASE
            WHEN ST_Contains(z.geom, ST_StartPoint(rl.geom)) THEN FALSE
            WHEN COALESCE(zph.point_inside_zone, FALSE) OR ST_Crosses(rl.geom, z.geom) THEN TRUE
            ELSE FALSE
        END AS zone_entry_pred
    FROM run_lines rl
    JOIN {schema}.context_zones z ON z.zone_name = ANY(%(zone_names)s)
    LEFT JOIN zone_point_hits zph
      ON zph.trajectory_id = rl.trajectory_id
     AND zph.zone_name = z.zone_name
),
corridor_point_hits AS (
    SELECT
        p.trajectory_id,
        c.corridor_name,
        BOOL_OR(ST_Contains(c.geom, p.geom)) AS point_inside_corridor
    FROM run_points p
    JOIN {schema}.context_corridors c
      ON c.corridor_name = %(corridor_name)s
     AND ST_Intersects(c.geom, p.geom)
    GROUP BY p.trajectory_id, c.corridor_name
),
corridor_preds AS (
    SELECT
        rl.trajectory_id,
        c.corridor_name,
        CASE
            WHEN COALESCE(cph.point_inside_corridor, FALSE) THEN TRUE
            WHEN ST_Intersects(rl.geom, c.geom)
                 AND ST_Length(ST_Intersection(rl.geom, c.geom)::geography) >= %(min_overlap_m)s THEN TRUE
            ELSE FALSE
        END AS corridor_pred
    FROM run_lines rl
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
    LEFT JOIN corridor_point_hits cph
      ON cph.trajectory_id = rl.trajectory_id
     AND cph.corridor_name = c.corridor_name
),
preds AS (
    SELECT
        zp.trajectory_id,
        zp.zone_name,
        cp.corridor_name,
        zp.zone_entry_pred,
        cp.corridor_pred
    FROM zone_preds zp
    JOIN corridor_preds cp
      ON cp.trajectory_id = zp.trajectory_id
)
"""
