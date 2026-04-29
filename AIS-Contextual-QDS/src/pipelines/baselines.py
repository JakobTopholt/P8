"""Simplification benchmark runner for baseline and B3 methods."""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from psycopg import Connection, sql

from ..config import AppConfig
from ..db import fetch_one
from ..evaluation.metrics import classification_metrics
from ..evaluation.strict_metrics import compute_strict_point_event_metrics
from ..query_semantics import build_run_prediction_ctes_sql, normalize_query_mode
from ..simplification import (
    B3PointEvidence,
    simplify_b3_indices,
    simplify_douglas_peucker_indices,
    simplify_uniform_indices,
)

LOGGER = logging.getLogger(__name__)

ALLOWED_METHODS = {"uniform", "dp", "b3"}


@dataclass(frozen=True)
class TrajectoryPoint:
    """One raw trajectory point record."""

    source_point_seq: int
    mmsi: int
    ts: dt.datetime
    lat: float
    lon: float


def _target_points(n_points: int, budget_ratio: float) -> int:
    target = int(round(budget_ratio * n_points))
    return max(2, min(n_points, target))


def _parse_methods(methods: list[str] | None, config: AppConfig) -> list[str]:
    raw_methods = methods or config.baselines.methods
    normalized = [method.strip().lower() for method in raw_methods if method.strip()]
    if not normalized:
        raise ValueError("No baseline methods configured.")

    unknown = [method for method in normalized if method not in ALLOWED_METHODS]
    if unknown:
        raise ValueError(f"Unknown baseline methods: {unknown}. Allowed: {sorted(ALLOWED_METHODS)}")

    ordered_unique: list[str] = []
    seen: set[str] = set()
    for method in normalized:
        if method not in seen:
            ordered_unique.append(method)
            seen.add(method)
    return ordered_unique


def _parse_budgets(budgets: list[float] | None, config: AppConfig) -> list[float]:
    values = budgets or config.queries.retained_point_budgets
    if not values:
        raise ValueError("No budgets configured.")

    parsed = [float(value) for value in values]
    for budget in parsed:
        if not 0 < budget <= 1:
            raise ValueError(f"Budget must be in (0, 1], got {budget}.")
    return sorted(parsed)


def _fetch_trajectories(
    conn: Connection[Any],
    config: AppConfig,
    *,
    split: str,
    subset_name: str,
) -> dict[int, list[TrajectoryPoint]]:
    schema = config.database.schema

    if split not in {"all", "dev", "eval"}:
        raise ValueError(f"Invalid split: {split!r}. Expected one of all/dev/eval.")

    if split == "all":
        sql = f"""
SELECT
    p.trajectory_id,
    p.point_seq,
    p.mmsi,
    p.ts,
    p.lat,
    p.lon
FROM {schema}.trajectory_points_raw p
ORDER BY p.trajectory_id, p.point_seq;
"""
        params: dict[str, Any] | None = None
    else:
        sql = f"""
SELECT
    p.trajectory_id,
    p.point_seq,
    p.mmsi,
    p.ts,
    p.lat,
    p.lon
FROM {schema}.trajectory_points_raw p
JOIN {schema}.trajectory_dev_eval_subset s
  ON s.trajectory_id = p.trajectory_id
WHERE s.subset_name = %(subset_name)s
  AND s.split = %(split)s
ORDER BY p.trajectory_id, p.point_seq;
"""
        params = {"subset_name": subset_name, "split": split}

    grouped: dict[int, list[TrajectoryPoint]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for trajectory_id, point_seq, mmsi, ts, lat, lon in cur:
            grouped.setdefault(int(trajectory_id), []).append(
                TrajectoryPoint(
                    source_point_seq=int(point_seq),
                    mmsi=int(mmsi),
                    ts=ts,
                    lat=float(lat),
                    lon=float(lon),
                )
            )

    if not grouped:
        raise RuntimeError(
            "No trajectories available for baseline run. "
            f"Check split='{split}' and subset_name='{subset_name}'."
        )

    return grouped


def _fetch_b3_point_evidence(
    conn: Connection[Any],
    config: AppConfig,
    trajectories: dict[int, list[TrajectoryPoint]],
) -> dict[int, list[B3PointEvidence]]:
    """Fetch B3-allowed query witnesses and local-shape evidence for selected trajectories."""
    if not trajectories:
        return {}

    schema = config.database.schema
    trajectory_ids = sorted(trajectories)
    feature_sql = f"""
WITH selected_points AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        p.geom,
        LAG(p.geom) OVER (PARTITION BY p.trajectory_id ORDER BY p.point_seq) AS prev_geom,
        LEAD(p.geom) OVER (PARTITION BY p.trajectory_id ORDER BY p.point_seq) AS next_geom
    FROM {schema}.trajectory_points_raw p
    WHERE p.trajectory_id = ANY(%(trajectory_ids)s::bigint[])
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
point_zone_hits AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        COALESCE(SUM(CASE WHEN ST_Covers(z.geom, p.geom) THEN 1 ELSE 0 END), 0)::integer AS zone_point_hits
    FROM selected_points p
    CROSS JOIN zones z
    GROUP BY p.trajectory_id, p.point_seq
),
point_corridor_hits AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        BOOL_OR(ST_Covers(c.geom, p.geom)) AS corridor_point_hit
    FROM selected_points p
    CROSS JOIN corridor c
    GROUP BY p.trajectory_id, p.point_seq
),
segments AS (
    SELECT
        p1.trajectory_id,
        p1.point_seq AS from_point_seq,
        p2.point_seq AS to_point_seq,
        ST_MakeLine(p1.geom, p2.geom) AS segment_geom,
        p1.geom AS from_geom,
        p2.geom AS to_geom
    FROM selected_points p1
    JOIN selected_points p2
      ON p2.trajectory_id = p1.trajectory_id
     AND p2.point_seq = p1.point_seq + 1
),
zone_segment_witnesses AS (
    SELECT
        s.trajectory_id,
        s.from_point_seq,
        s.to_point_seq,
        COALESCE(
            SUM(
                CASE
                    WHEN (NOT ST_Contains(z.geom, s.from_geom) AND ST_Contains(z.geom, s.to_geom))
                         OR ST_Crosses(s.segment_geom, z.geom)
                    THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS zone_entry_segment_witnesses,
        COALESCE(
            SUM(
                CASE
                    WHEN ST_Covers(z.geom, s.from_geom) IS DISTINCT FROM ST_Covers(z.geom, s.to_geom) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS zone_transition_witnesses,
        COALESCE(
            SUM(
                CASE
                    WHEN NOT ST_Covers(z.geom, s.from_geom) AND ST_Covers(z.geom, s.to_geom) THEN 1
                    ELSE 0
                END
            ),
            0
        )::integer AS zone_entry_event_witnesses
    FROM segments s
    CROSS JOIN zones z
    GROUP BY s.trajectory_id, s.from_point_seq, s.to_point_seq
),
corridor_segment_witnesses AS (
    SELECT
        s.trajectory_id,
        s.from_point_seq,
        s.to_point_seq,
        CASE
            WHEN BOOL_OR(
                CASE
                    WHEN ST_Intersects(s.segment_geom, c.geom)
                    THEN ST_Length(ST_Intersection(s.segment_geom, c.geom)::geography) >= %(min_overlap_m)s
                    ELSE FALSE
                END
            ) THEN 1
            ELSE 0
        END AS corridor_segment_witnesses,
        BOOL_OR(ST_Covers(c.geom, s.from_geom) IS DISTINCT FROM ST_Covers(c.geom, s.to_geom))
            AS corridor_transition_witness,
        BOOL_OR(NOT ST_Covers(c.geom, s.from_geom) AND ST_Covers(c.geom, s.to_geom))
            AS corridor_entry_event_witness
    FROM segments s
    CROSS JOIN corridor c
    GROUP BY s.trajectory_id, s.from_point_seq, s.to_point_seq
),
segment_endpoint_witnesses AS (
    SELECT
        trajectory_id,
        point_seq,
        COALESCE(SUM(zone_entry_segment_witnesses), 0)::integer AS zone_entry_segment_witnesses,
        COALESCE(SUM(zone_transition_witnesses), 0)::integer AS zone_transition_witnesses,
        COALESCE(SUM(zone_entry_event_witnesses), 0)::integer AS zone_entry_event_witnesses,
        COALESCE(SUM(corridor_segment_witnesses), 0)::integer AS corridor_segment_witnesses,
        BOOL_OR(corridor_transition_witness) AS corridor_transition_witness,
        BOOL_OR(corridor_entry_event_witness) AS corridor_entry_event_witness
    FROM (
        SELECT
            z.trajectory_id,
            z.from_point_seq AS point_seq,
            z.zone_entry_segment_witnesses,
            z.zone_transition_witnesses,
            z.zone_entry_event_witnesses,
            COALESCE(c.corridor_segment_witnesses, 0) AS corridor_segment_witnesses,
            COALESCE(c.corridor_transition_witness, FALSE) AS corridor_transition_witness,
            COALESCE(c.corridor_entry_event_witness, FALSE) AS corridor_entry_event_witness
        FROM zone_segment_witnesses z
        LEFT JOIN corridor_segment_witnesses c
          ON c.trajectory_id = z.trajectory_id
         AND c.from_point_seq = z.from_point_seq
         AND c.to_point_seq = z.to_point_seq
        UNION ALL
        SELECT
            z.trajectory_id,
            z.to_point_seq AS point_seq,
            z.zone_entry_segment_witnesses,
            z.zone_transition_witnesses,
            z.zone_entry_event_witnesses,
            COALESCE(c.corridor_segment_witnesses, 0) AS corridor_segment_witnesses,
            COALESCE(c.corridor_transition_witness, FALSE) AS corridor_transition_witness,
            COALESCE(c.corridor_entry_event_witness, FALSE) AS corridor_entry_event_witness
        FROM zone_segment_witnesses z
        LEFT JOIN corridor_segment_witnesses c
          ON c.trajectory_id = z.trajectory_id
         AND c.from_point_seq = z.from_point_seq
         AND c.to_point_seq = z.to_point_seq
    ) endpoint_rows
    GROUP BY trajectory_id, point_seq
),
local_shape AS (
    SELECT
        p.trajectory_id,
        p.point_seq,
        CASE
            WHEN p.prev_geom IS NULL OR p.next_geom IS NULL THEN 0.0::double precision
            ELSE COALESCE(
                DEGREES(
                    ABS(
                        ATAN2(
                            SIN(ST_Azimuth(p.geom, p.next_geom) - ST_Azimuth(p.prev_geom, p.geom)),
                            COS(ST_Azimuth(p.geom, p.next_geom) - ST_Azimuth(p.prev_geom, p.geom))
                        )
                    )
                ),
                0.0
            )
        END AS local_turn_degrees,
        CASE
            WHEN p.prev_geom IS NULL OR p.next_geom IS NULL OR ST_Equals(p.prev_geom, p.next_geom)
                THEN 0.0::double precision
            ELSE COALESCE(
                ST_Distance(p.geom::geography, ST_MakeLine(p.prev_geom, p.next_geom)::geography),
                0.0
            )
        END AS local_deviation_m
    FROM selected_points p
)
SELECT
    p.trajectory_id,
    p.point_seq,
    COALESCE(pzh.zone_point_hits, 0) AS zone_point_hits,
    COALESCE(sew.zone_entry_segment_witnesses, 0) AS zone_entry_segment_witnesses,
    COALESCE(sew.zone_transition_witnesses, 0) AS zone_transition_witnesses,
    COALESCE(sew.zone_entry_event_witnesses, 0) AS zone_entry_event_witnesses,
    COALESCE(pch.corridor_point_hit, FALSE) AS corridor_point_hit,
    COALESCE(sew.corridor_segment_witnesses, 0) AS corridor_segment_witnesses,
    COALESCE(sew.corridor_transition_witness, FALSE) AS corridor_transition_witness,
    COALESCE(sew.corridor_entry_event_witness, FALSE) AS corridor_entry_event_witness,
    COALESCE(ls.local_turn_degrees, 0.0) AS local_turn_degrees,
    COALESCE(ls.local_deviation_m, 0.0) AS local_deviation_m
FROM selected_points p
LEFT JOIN point_zone_hits pzh
  ON pzh.trajectory_id = p.trajectory_id
 AND pzh.point_seq = p.point_seq
LEFT JOIN point_corridor_hits pch
  ON pch.trajectory_id = p.trajectory_id
 AND pch.point_seq = p.point_seq
LEFT JOIN segment_endpoint_witnesses sew
  ON sew.trajectory_id = p.trajectory_id
 AND sew.point_seq = p.point_seq
LEFT JOIN local_shape ls
  ON ls.trajectory_id = p.trajectory_id
 AND ls.point_seq = p.point_seq
ORDER BY p.trajectory_id, p.point_seq;
"""

    evidence_by_seq: dict[int, dict[int, B3PointEvidence]] = {}
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET LOCAL work_mem = {};").format(sql.Literal("128MB")))
        cur.execute(
            feature_sql,
            {
                "trajectory_ids": trajectory_ids,
                "zone_names": config.context.zone_names,
                "corridor_name": config.context.corridor_name,
                "min_overlap_m": config.queries.min_corridor_overlap_meters,
            },
        )
        for (
            trajectory_id,
            point_seq,
            zone_point_hits,
            zone_entry_segment_witnesses,
            zone_transition_witnesses,
            zone_entry_event_witnesses,
            corridor_point_hit,
            corridor_segment_witnesses,
            corridor_transition_witness,
            corridor_entry_event_witness,
            local_turn_degrees,
            local_deviation_m,
        ) in cur:
            evidence_by_seq.setdefault(int(trajectory_id), {})[int(point_seq)] = B3PointEvidence(
                zone_point_hits=int(zone_point_hits),
                zone_entry_segment_witnesses=int(zone_entry_segment_witnesses),
                zone_transition_witnesses=int(zone_transition_witnesses),
                zone_entry_event_witnesses=int(zone_entry_event_witnesses),
                corridor_point_hit=bool(corridor_point_hit),
                corridor_segment_witnesses=int(corridor_segment_witnesses),
                corridor_transition_witness=bool(corridor_transition_witness),
                corridor_entry_event_witness=bool(corridor_entry_event_witness),
                local_turn_degrees=float(local_turn_degrees or 0.0),
                local_deviation_m=float(local_deviation_m or 0.0),
            )

    features: dict[int, list[B3PointEvidence]] = {}
    for trajectory_id, points in trajectories.items():
        sequence_features = evidence_by_seq.get(trajectory_id, {})
        features[trajectory_id] = [
            sequence_features.get(point.source_point_seq, B3PointEvidence())
            for point in points
        ]

    return features


def _simplify_indices(
    method: str,
    trajectory: list[TrajectoryPoint],
    target_points: int,
    *,
    dp_search_iterations: int,
    b3_evidence: list[B3PointEvidence] | None = None,
) -> list[int]:
    if method == "uniform":
        return simplify_uniform_indices(len(trajectory), target_points)

    if method == "dp":
        points = [(point.lon, point.lat) for point in trajectory]
        return simplify_douglas_peucker_indices(
            points,
            target_points,
            search_iterations=dp_search_iterations,
        )

    if method == "b3":
        if b3_evidence is None:
            raise ValueError("B3 simplification requires point evidence.")
        return simplify_b3_indices(b3_evidence, target_points)

    raise ValueError(f"Unknown method: {method}")


def _ensure_truth_labels_available(
    conn: Connection[Any],
    config: AppConfig,
    *,
    truth_label_mode: str,
) -> None:
    schema = config.database.schema
    trajectory_count = int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.trajectories_raw;") or 0)
    if trajectory_count <= 0:
        raise RuntimeError("No raw trajectories available. Run prepare-data or build-trajectories first.")

    expected_rows = trajectory_count * len(config.context.zone_names)
    actual_rows = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_query_labels "
                "WHERE label_mode = %(label_mode)s AND corridor_name = %(corridor_name)s;"
            ),
            {
                "label_mode": truth_label_mode,
                "corridor_name": config.context.corridor_name,
            },
        )
        or 0
    )
    if actual_rows != expected_rows:
        raise RuntimeError(
            "Truth labels are missing or incomplete for "
            f"label_mode={truth_label_mode!r}. Expected {expected_rows} rows, found {actual_rows}. "
            f"Run `python -m src.cli compute-labels --mode {truth_label_mode}` first."
        )


def _build_run_metadata(
    config: AppConfig,
    *,
    config_path: str,
    trajectory_split: str,
    subset_name: str,
    evaluation_mode: str,
    truth_label_mode: str,
) -> dict[str, object]:
    return {
        "project_name": config.project.name,
        "database_schema": config.database.schema,
        "source_points_table": config.database.source_points_table,
        "region_name": config.scope.region_name,
        "window_start": config.scope.window_start,
        "window_end": config.scope.window_end,
        "vessel_class_pattern": config.scope.vessel_class_pattern,
        "zone_names": list(config.context.zone_names),
        "corridor_name": config.context.corridor_name,
        "trajectory_split": trajectory_split,
        "subset_name": subset_name,
        "evaluation_mode": evaluation_mode,
        "truth_label_mode": truth_label_mode,
        "session_profile": config.performance.session_profile,
        "config_path": config_path,
    }


def _create_run_record(
    conn: Connection[Any],
    *,
    schema: str,
    run_tag: str,
    method: str,
    budget: float,
    config_path: str,
    evaluation_mode: str,
    truth_label_mode: str,
    trajectory_split: str,
    subset_name: str,
    run_metadata: dict[str, object],
    overwrite: bool,
) -> int:
    existing_sql = (
        f"SELECT run_id FROM {schema}.simplification_runs "
        "WHERE run_tag = %(run_tag)s AND method_name = %(method)s AND budget_ratio = %(budget)s "
        "  AND evaluation_mode = %(evaluation_mode)s AND truth_label_mode = %(truth_label_mode)s "
        "  AND trajectory_split = %(trajectory_split)s AND subset_name = %(subset_name)s;"
    )
    with conn.cursor() as cur:
        cur.execute(
            existing_sql,
            {
                "run_tag": run_tag,
                "method": method,
                "budget": budget,
                "evaluation_mode": evaluation_mode,
                "truth_label_mode": truth_label_mode,
                "trajectory_split": trajectory_split,
                "subset_name": subset_name,
            },
        )
        row = cur.fetchone()

    if row is not None:
        run_id = int(row[0])
        if not overwrite:
            raise RuntimeError(
                f"Run already exists for run_tag={run_tag!r}, method={method!r}, budget={budget}. "
                "Use --overwrite to replace it."
            )
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {schema}.simplification_runs WHERE run_id = %(run_id)s;", {"run_id": run_id})

    with conn.cursor() as cur:
        cur.execute(
            (
                f"INSERT INTO {schema}.simplification_runs "
                "(run_tag, method_name, budget_ratio, config_path, evaluation_mode, truth_label_mode, trajectory_split, subset_name, run_metadata) "
                "VALUES (%(run_tag)s, %(method)s, %(budget)s, %(config_path)s, %(evaluation_mode)s, %(truth_label_mode)s, %(trajectory_split)s, %(subset_name)s, %(run_metadata)s::jsonb) "
                "RETURNING run_id;"
            ),
            {
                "run_tag": run_tag,
                "method": method,
                "budget": budget,
                "config_path": config_path,
                "evaluation_mode": evaluation_mode,
                "truth_label_mode": truth_label_mode,
                "trajectory_split": trajectory_split,
                "subset_name": subset_name,
                "run_metadata": json.dumps(run_metadata, sort_keys=True),
            },
        )
        return int(cur.fetchone()[0])


def _flush_simplified_buffer(conn: Connection[Any], schema: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    insert_sql = f"""
INSERT INTO {schema}.trajectories_simplified_points (
    run_id,
    trajectory_id,
    point_seq,
    source_point_seq,
    mmsi,
    ts,
    lat,
    lon,
    geom
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
);
"""

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)


def _materialize_simplified_segments(conn: Connection[Any], schema: str, run_id: int) -> int:
    """Persist adjacent simplified-point segments for exact evaluation reuse."""
    insert_sql = f"""
INSERT INTO {schema}.trajectories_simplified_segments (
    run_id,
    trajectory_id,
    segment_seq,
    from_point_seq,
    to_point_seq,
    from_source_point_seq,
    to_source_point_seq,
    geom,
    from_geom,
    to_geom
)
SELECT
    run_id,
    trajectory_id,
    point_seq AS segment_seq,
    point_seq AS from_point_seq,
    next_point_seq AS to_point_seq,
    source_point_seq AS from_source_point_seq,
    next_source_point_seq AS to_source_point_seq,
    ST_MakeLine(geom, next_geom) AS geom,
    geom AS from_geom,
    next_geom AS to_geom
FROM (
    SELECT
        run_id,
        trajectory_id,
        point_seq,
        source_point_seq,
        geom,
        LEAD(point_seq) OVER (PARTITION BY run_id, trajectory_id ORDER BY point_seq) AS next_point_seq,
        LEAD(source_point_seq) OVER (PARTITION BY run_id, trajectory_id ORDER BY point_seq) AS next_source_point_seq,
        LEAD(geom) OVER (PARTITION BY run_id, trajectory_id ORDER BY point_seq) AS next_geom
    FROM {schema}.trajectories_simplified_points
    WHERE run_id = %(run_id)s
) ordered_points
WHERE next_geom IS NOT NULL;
"""
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {schema}.trajectories_simplified_segments WHERE run_id = %(run_id)s;",
            {"run_id": run_id},
        )
        cur.execute(insert_sql, {"run_id": run_id})
        cur.execute(
            f"SELECT COUNT(*) FROM {schema}.trajectories_simplified_segments WHERE run_id = %(run_id)s;",
            {"run_id": run_id},
        )
        segment_count = int(cur.fetchone()[0])

    with conn.cursor() as cur:
        cur.execute(f"ANALYZE {schema}.trajectories_simplified_segments;")

    return segment_count


def _safe_metric_key(raw_value: str) -> str:
    """Normalize dynamic metric-key fragments for benchmark_metrics."""
    return "".join(char if char.isalnum() else "_" for char in raw_value.lower()).strip("_")


def _empty_confusion_counts() -> dict[str, int]:
    return {
        "tp": 0,
        "fp": 0,
        "fn": 0,
        "tn": 0,
        "support": 0,
        "true_positive": 0,
        "predicted_positive": 0,
    }


def _compute_run_metric_counts(
    conn: Connection[Any],
    config: AppConfig,
    run_id: int,
    *,
    evaluation_mode: str,
    truth_label_mode: str,
) -> dict[str, dict[str, dict[str, int]]]:
    schema = config.database.schema

    prediction_ctes = build_run_prediction_ctes_sql(
        schema,
        mode=evaluation_mode,
        run_points_where_sql="WHERE run_id = %(run_id)s",
    )
    metric_sql = f"""
WITH {prediction_ctes},
truth AS (
    SELECT
        q.trajectory_id,
        q.zone_name,
        q.corridor_name,
        q.zone_entry AS zone_true,
        q.corridor_membership AS corridor_true
    FROM {schema}.trajectory_query_labels q
    JOIN run_lines rl ON rl.trajectory_id = q.trajectory_id
    WHERE q.zone_name = ANY(%(zone_names)s)
      AND q.corridor_name = %(corridor_name)s
      AND q.label_mode = %(truth_label_mode)s
),
pairs AS (
    SELECT
        t.trajectory_id,
        t.zone_name,
        t.zone_true,
        zp.zone_entry_pred AS zone_pred,
        t.corridor_true,
        cp.corridor_pred
    FROM truth t
    JOIN zone_preds zp
      ON zp.trajectory_id = t.trajectory_id
     AND zp.zone_name = t.zone_name
    JOIN corridor_preds cp
      ON cp.trajectory_id = t.trajectory_id
     AND cp.corridor_name = t.corridor_name
),
corridor_pairs AS (
    SELECT DISTINCT
        trajectory_id,
        corridor_true,
        corridor_pred
    FROM pairs
),
metric_rows AS (
    SELECT
        'zone_entry' AS family,
        'all' AS group_key,
        COALESCE(SUM(CASE WHEN zone_true AND zone_pred THEN 1 ELSE 0 END), 0) AS tp,
        COALESCE(SUM(CASE WHEN NOT zone_true AND zone_pred THEN 1 ELSE 0 END), 0) AS fp,
        COALESCE(SUM(CASE WHEN zone_true AND NOT zone_pred THEN 1 ELSE 0 END), 0) AS fn,
        COALESCE(SUM(CASE WHEN NOT zone_true AND NOT zone_pred THEN 1 ELSE 0 END), 0) AS tn,
        COALESCE(COUNT(*), 0) AS support,
        COALESCE(SUM(CASE WHEN zone_true THEN 1 ELSE 0 END), 0) AS true_positive,
        COALESCE(SUM(CASE WHEN zone_pred THEN 1 ELSE 0 END), 0) AS predicted_positive
    FROM pairs
    UNION ALL
    SELECT
        'zone_entry' AS family,
        zone_name AS group_key,
        COALESCE(SUM(CASE WHEN zone_true AND zone_pred THEN 1 ELSE 0 END), 0) AS tp,
        COALESCE(SUM(CASE WHEN NOT zone_true AND zone_pred THEN 1 ELSE 0 END), 0) AS fp,
        COALESCE(SUM(CASE WHEN zone_true AND NOT zone_pred THEN 1 ELSE 0 END), 0) AS fn,
        COALESCE(SUM(CASE WHEN NOT zone_true AND NOT zone_pred THEN 1 ELSE 0 END), 0) AS tn,
        COALESCE(COUNT(*), 0) AS support,
        COALESCE(SUM(CASE WHEN zone_true THEN 1 ELSE 0 END), 0) AS true_positive,
        COALESCE(SUM(CASE WHEN zone_pred THEN 1 ELSE 0 END), 0) AS predicted_positive
    FROM pairs
    GROUP BY zone_name
    UNION ALL
    SELECT
        'corridor_membership' AS family,
        'all' AS group_key,
        COALESCE(SUM(CASE WHEN corridor_true AND corridor_pred THEN 1 ELSE 0 END), 0) AS tp,
        COALESCE(SUM(CASE WHEN NOT corridor_true AND corridor_pred THEN 1 ELSE 0 END), 0) AS fp,
        COALESCE(SUM(CASE WHEN corridor_true AND NOT corridor_pred THEN 1 ELSE 0 END), 0) AS fn,
        COALESCE(SUM(CASE WHEN NOT corridor_true AND NOT corridor_pred THEN 1 ELSE 0 END), 0) AS tn,
        COALESCE(COUNT(*), 0) AS support,
        COALESCE(SUM(CASE WHEN corridor_true THEN 1 ELSE 0 END), 0) AS true_positive,
        COALESCE(SUM(CASE WHEN corridor_pred THEN 1 ELSE 0 END), 0) AS predicted_positive
    FROM corridor_pairs
)
SELECT
    family,
    group_key,
    tp,
    fp,
    fn,
    tn,
    support,
    true_positive,
    predicted_positive
FROM metric_rows
ORDER BY family, group_key;
"""

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET LOCAL work_mem = {};").format(
                sql.Literal("128MB" if evaluation_mode == "segment_exact" else "64MB")
            )
        )
        cur.execute(
            metric_sql,
            {
                "run_id": run_id,
                "zone_names": config.context.zone_names,
                "corridor_name": config.context.corridor_name,
                "truth_label_mode": truth_label_mode,
                "min_overlap_m": config.queries.min_corridor_overlap_meters,
            },
        )
        rows = cur.fetchall()

    counts: dict[str, dict[str, dict[str, int]]] = {
        "zone_entry": {"all": _empty_confusion_counts()},
        "corridor_membership": {"all": _empty_confusion_counts()},
    }
    for family, group_key, tp, fp, fn, tn, support, true_positive, predicted_positive in rows:
        family_key = str(family)
        counts.setdefault(family_key, {})
        counts[family_key][str(group_key)] = {
            "tp": int(tp),
            "fp": int(fp),
            "fn": int(fn),
            "tn": int(tn),
            "support": int(support),
            "true_positive": int(true_positive),
            "predicted_positive": int(predicted_positive),
        }
    return counts


def _compute_retention_metrics(conn: Connection[Any], schema: str, run_id: int) -> dict[str, float]:
    simplified_points = int(
        fetch_one(
            conn,
            f"SELECT COUNT(*) FROM {schema}.trajectories_simplified_points WHERE run_id = %(run_id)s;",
            {"run_id": run_id},
        )
        or 0
    )

    raw_points = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(*) FROM {schema}.trajectory_points_raw p "
                f"JOIN (SELECT DISTINCT trajectory_id FROM {schema}.trajectories_simplified_points "
                "      WHERE run_id = %(run_id)s) r USING (trajectory_id);"
            ),
            {"run_id": run_id},
        )
        or 0
    )

    trajectory_count = int(
        fetch_one(
            conn,
            (
                f"SELECT COUNT(DISTINCT trajectory_id) "
                f"FROM {schema}.trajectories_simplified_points WHERE run_id = %(run_id)s;"
            ),
            {"run_id": run_id},
        )
        or 0
    )

    retained_ratio = float(simplified_points) / float(raw_points) if raw_points > 0 else 0.0
    return {
        "simplified_points": float(simplified_points),
        "raw_points": float(raw_points),
        "retained_point_ratio": retained_ratio,
        "trajectory_count": float(trajectory_count),
    }


def _store_metrics(conn: Connection[Any], schema: str, run_id: int, metrics: dict[str, float]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.benchmark_metrics WHERE run_id = %(run_id)s;", {"run_id": run_id})
        cur.executemany(
            (
                f"INSERT INTO {schema}.benchmark_metrics (run_id, metric_key, metric_value) "
                "VALUES (%s, %s, %s);"
            ),
            [(run_id, key, value) for key, value in metrics.items()],
        )


def run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    config_path: str | None = None,
    methods: list[str] | None = None,
    budgets: list[float] | None = None,
    run_tag: str | None = None,
    split: str | None = None,
    subset_name: str | None = None,
    evaluation_mode: str | None = None,
    truth_label_mode: str | None = None,
    overwrite: bool = False,
) -> list[dict[str, float | int | str]]:
    """Run simplification methods for all requested budgets and store metrics."""
    schema = config.database.schema
    selected_methods = _parse_methods(methods, config)
    selected_budgets = _parse_budgets(budgets, config)
    resolved_evaluation_mode = normalize_query_mode(evaluation_mode, default=config.performance.evaluation_mode)
    resolved_truth_label_mode = normalize_query_mode(truth_label_mode, default=resolved_evaluation_mode)

    selected_split = split or config.baselines.default_split
    selected_subset_name = subset_name or config.subsets.subset_name
    stored_subset_name = "" if selected_split == "all" else selected_subset_name
    current_run_tag = run_tag or dt.datetime.now(dt.timezone.utc).strftime(
        f"benchmark_{selected_split}_{resolved_evaluation_mode}_truth_{resolved_truth_label_mode}_%Y%m%dT%H%M%SZ"
    )

    LOGGER.info(
        "Running benchmark methods=%s budgets=%s split=%s subset_name=%s run_tag=%s evaluation_mode=%s truth_label_mode=%s",
        selected_methods,
        selected_budgets,
        selected_split,
        selected_subset_name,
        current_run_tag,
        resolved_evaluation_mode,
        resolved_truth_label_mode,
    )

    _ensure_truth_labels_available(
        conn,
        config,
        truth_label_mode=resolved_truth_label_mode,
    )

    trajectories = _fetch_trajectories(
        conn,
        config,
        split=selected_split,
        subset_name=selected_subset_name,
    )
    LOGGER.info("Loaded %s trajectories for simplification benchmark.", len(trajectories))
    b3_evidence_by_trajectory = (
        _fetch_b3_point_evidence(conn, config, trajectories)
        if "b3" in selected_methods
        else {}
    )
    if b3_evidence_by_trajectory:
        LOGGER.info("Loaded B3 point evidence for %s trajectories.", len(b3_evidence_by_trajectory))

    results: list[dict[str, float | int | str]] = []
    for method in selected_methods:
        for budget in selected_budgets:
            run_id = _create_run_record(
                conn,
                schema=schema,
                run_tag=current_run_tag,
                method=method,
                budget=budget,
                config_path=(config_path or "unspecified"),
                evaluation_mode=resolved_evaluation_mode,
                truth_label_mode=resolved_truth_label_mode,
                trajectory_split=selected_split,
                subset_name=stored_subset_name,
                run_metadata=_build_run_metadata(
                    config,
                    config_path=(config_path or "unspecified"),
                    trajectory_split=selected_split,
                    subset_name=stored_subset_name,
                    evaluation_mode=resolved_evaluation_mode,
                    truth_label_mode=resolved_truth_label_mode,
                ),
                overwrite=overwrite,
            )

            start = perf_counter()
            buffer: list[tuple[Any, ...]] = []
            for trajectory_id, points in trajectories.items():
                target_points = _target_points(len(points), budget)
                kept_indices = _simplify_indices(
                    method,
                    points,
                    target_points,
                    dp_search_iterations=config.baselines.dp_search_iterations,
                    b3_evidence=b3_evidence_by_trajectory.get(trajectory_id),
                )

                for out_seq, source_idx in enumerate(kept_indices, start=1):
                    point = points[source_idx]
                    buffer.append(
                        (
                            run_id,
                            trajectory_id,
                            out_seq,
                            point.source_point_seq,
                            point.mmsi,
                            point.ts,
                            point.lat,
                            point.lon,
                            point.lon,
                            point.lat,
                        )
                    )

                if len(buffer) >= config.baselines.insert_batch_size:
                    _flush_simplified_buffer(conn, schema, buffer)
                    buffer.clear()

            _flush_simplified_buffer(conn, schema, buffer)
            segment_count = _materialize_simplified_segments(conn, schema, run_id)
            elapsed_seconds = perf_counter() - start
            LOGGER.info("Materialized %s simplified segments for run_id=%s.", segment_count, run_id)

            counts = _compute_run_metric_counts(
                conn,
                config,
                run_id,
                evaluation_mode=resolved_evaluation_mode,
                truth_label_mode=resolved_truth_label_mode,
            )
            zone_counts = counts["zone_entry"]["all"]
            corridor_counts = counts["corridor_membership"]["all"]
            zone_metrics = classification_metrics(
                zone_counts["tp"],
                zone_counts["fp"],
                zone_counts["fn"],
            )
            corridor_metrics = classification_metrics(
                corridor_counts["tp"],
                corridor_counts["fp"],
                corridor_counts["fn"],
            )
            retention = _compute_retention_metrics(conn, schema, run_id)

            metric_payload: dict[str, float] = {
                "zone_entry_precision": zone_metrics["precision"],
                "zone_entry_recall": zone_metrics["recall"],
                "zone_entry_f1": zone_metrics["f1"],
                "zone_entry_tp": float(zone_counts["tp"]),
                "zone_entry_fp": float(zone_counts["fp"]),
                "zone_entry_fn": float(zone_counts["fn"]),
                "zone_entry_tn": float(zone_counts["tn"]),
                "zone_entry_support": float(zone_counts["support"]),
                "zone_entry_true_positive": float(zone_counts["true_positive"]),
                "zone_entry_predicted_positive": float(zone_counts["predicted_positive"]),
                "corridor_membership_precision": corridor_metrics["precision"],
                "corridor_membership_recall": corridor_metrics["recall"],
                "corridor_membership_f1": corridor_metrics["f1"],
                "corridor_membership_tp": float(corridor_counts["tp"]),
                "corridor_membership_fp": float(corridor_counts["fp"]),
                "corridor_membership_fn": float(corridor_counts["fn"]),
                "corridor_membership_tn": float(corridor_counts["tn"]),
                "corridor_membership_support": float(corridor_counts["support"]),
                "corridor_membership_true_positive": float(corridor_counts["true_positive"]),
                "corridor_membership_predicted_positive": float(corridor_counts["predicted_positive"]),
                "retained_point_ratio": retention["retained_point_ratio"],
                "simplification_runtime_seconds": elapsed_seconds,
                "n_query_pairs": float(zone_counts["support"]),
                "n_corridor_trajectories": float(corridor_counts["support"]),
                "n_simplified_trajectories": retention["trajectory_count"],
                "n_simplified_points": retention["simplified_points"],
                "n_simplified_segments": float(segment_count),
                "n_raw_points": retention["raw_points"],
            }
            line_zone_metric_values: list[dict[str, float]] = []
            for zone_name in config.context.zone_names:
                per_zone_counts = counts["zone_entry"].get(zone_name, _empty_confusion_counts())
                per_zone_metrics = classification_metrics(
                    per_zone_counts["tp"],
                    per_zone_counts["fp"],
                    per_zone_counts["fn"],
                )
                line_zone_metric_values.append(per_zone_metrics)
                zone_key = _safe_metric_key(zone_name)
                metric_payload.update(
                    {
                        f"zone_entry_{zone_key}_precision": per_zone_metrics["precision"],
                        f"zone_entry_{zone_key}_recall": per_zone_metrics["recall"],
                        f"zone_entry_{zone_key}_f1": per_zone_metrics["f1"],
                        f"zone_entry_{zone_key}_tp": float(per_zone_counts["tp"]),
                        f"zone_entry_{zone_key}_fp": float(per_zone_counts["fp"]),
                        f"zone_entry_{zone_key}_fn": float(per_zone_counts["fn"]),
                        f"zone_entry_{zone_key}_tn": float(per_zone_counts["tn"]),
                        f"zone_entry_{zone_key}_support": float(per_zone_counts["support"]),
                        f"zone_entry_{zone_key}_true_positive": float(per_zone_counts["true_positive"]),
                        f"zone_entry_{zone_key}_predicted_positive": float(per_zone_counts["predicted_positive"]),
                    }
                )
            if line_zone_metric_values:
                metric_payload["zone_entry_macro_precision"] = sum(
                    item["precision"] for item in line_zone_metric_values
                ) / len(line_zone_metric_values)
                metric_payload["zone_entry_macro_recall"] = sum(
                    item["recall"] for item in line_zone_metric_values
                ) / len(line_zone_metric_values)
                metric_payload["zone_entry_macro_f1"] = sum(
                    item["f1"] for item in line_zone_metric_values
                ) / len(line_zone_metric_values)
            metric_payload.update(compute_strict_point_event_metrics(conn, config, run_id))
            _store_metrics(conn, schema, run_id, metric_payload)

            result_row: dict[str, float | int | str] = {
                "run_id": run_id,
                "run_tag": current_run_tag,
                "method": method,
                "evaluation_mode": resolved_evaluation_mode,
                "truth_label_mode": resolved_truth_label_mode,
                "trajectory_split": selected_split,
                "subset_name": stored_subset_name,
                "budget": budget,
                **metric_payload,
            }
            results.append(result_row)
            LOGGER.info(
                "Finished run_id=%s method=%s budget=%.3f zone_f1=%.4f corridor_f1=%.4f retained=%.4f",
                run_id,
                method,
                budget,
                zone_metrics["f1"],
                corridor_metrics["f1"],
                retention["retained_point_ratio"],
            )
            conn.commit()

    return results
