"""Sprint 2 baseline simplification runner."""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import fetch_one
from ..evaluation.metrics import classification_metrics
from ..simplification import simplify_douglas_peucker_indices, simplify_uniform_indices

LOGGER = logging.getLogger(__name__)

ALLOWED_METHODS = {"uniform", "dp"}


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


def _simplify_indices(
    method: str,
    trajectory: list[TrajectoryPoint],
    target_points: int,
    *,
    dp_search_iterations: int,
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

    raise ValueError(f"Unknown method: {method}")


def _create_run_record(
    conn: Connection[Any],
    *,
    schema: str,
    run_tag: str,
    method: str,
    budget: float,
    config_path: str,
    overwrite: bool,
) -> int:
    existing_sql = (
        f"SELECT run_id FROM {schema}.simplification_runs "
        "WHERE run_tag = %(run_tag)s AND method_name = %(method)s AND budget_ratio = %(budget)s;"
    )
    with conn.cursor() as cur:
        cur.execute(
            existing_sql,
            {"run_tag": run_tag, "method": method, "budget": budget},
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
                "(run_tag, method_name, budget_ratio, config_path) "
                "VALUES (%(run_tag)s, %(method)s, %(budget)s, %(config_path)s) "
                "RETURNING run_id;"
            ),
            {
                "run_tag": run_tag,
                "method": method,
                "budget": budget,
                "config_path": config_path,
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


def _compute_run_metric_counts(conn: Connection[Any], config: AppConfig, run_id: int) -> dict[str, int]:
    schema = config.database.schema

    metric_sql = f"""
WITH run_points AS (
    SELECT trajectory_id, point_seq, geom
    FROM {schema}.trajectories_simplified_points
    WHERE run_id = %(run_id)s
),
run_traj AS (
    SELECT DISTINCT trajectory_id
    FROM run_points
),
first_points AS (
    SELECT trajectory_id, geom
    FROM run_points
    WHERE point_seq = 1
),
segments AS (
    SELECT
        p1.trajectory_id,
        ST_MakeLine(p1.geom, p2.geom) AS segment_geom,
        p1.geom AS from_geom,
        p2.geom AS to_geom
    FROM run_points p1
    JOIN run_points p2
      ON p2.trajectory_id = p1.trajectory_id
     AND p2.point_seq = p1.point_seq + 1
),
zone_preds AS (
    SELECT
        rt.trajectory_id,
        z.zone_name,
        CASE
            WHEN ST_Contains(z.geom, fp.geom) THEN FALSE
            WHEN EXISTS (
                SELECT 1
                FROM segments s
                WHERE s.trajectory_id = rt.trajectory_id
                  AND (
                      (NOT ST_Contains(z.geom, s.from_geom) AND ST_Contains(z.geom, s.to_geom))
                      OR ST_Crosses(s.segment_geom, z.geom)
                  )
            ) THEN TRUE
            ELSE FALSE
        END AS zone_entry_pred
    FROM run_traj rt
    JOIN first_points fp ON fp.trajectory_id = rt.trajectory_id
    JOIN {schema}.context_zones z ON z.zone_name = ANY(%(zone_names)s)
),
point_corridor_hits AS (
    SELECT
        p.trajectory_id,
        BOOL_OR(ST_Covers(c.geom, p.geom)) AS corridor_hit_by_point
    FROM run_points p
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
    GROUP BY p.trajectory_id
),
segment_corridor_hits AS (
    SELECT
        s.trajectory_id,
        BOOL_OR(
            ST_Intersects(s.segment_geom, c.geom)
            AND ST_Length(ST_Intersection(s.segment_geom, c.geom)::geography) >= %(min_overlap_m)s
        ) AS corridor_hit_by_segment
    FROM segments s
    JOIN {schema}.context_corridors c ON c.corridor_name = %(corridor_name)s
    GROUP BY s.trajectory_id
),
preds AS (
    SELECT
        zp.trajectory_id,
        zp.zone_name,
        %(corridor_name)s::text AS corridor_name,
        zp.zone_entry_pred AS zone_pred,
        COALESCE(pch.corridor_hit_by_point, FALSE) OR COALESCE(sch.corridor_hit_by_segment, FALSE) AS corridor_pred
    FROM zone_preds zp
    LEFT JOIN point_corridor_hits pch
      ON pch.trajectory_id = zp.trajectory_id
    LEFT JOIN segment_corridor_hits sch
      ON sch.trajectory_id = zp.trajectory_id
),
truth AS (
    SELECT
        q.trajectory_id,
        q.zone_name,
        q.corridor_name,
        q.zone_entry AS zone_true,
        q.corridor_membership AS corridor_true
    FROM {schema}.trajectory_query_labels q
    JOIN run_traj rt ON rt.trajectory_id = q.trajectory_id
    WHERE q.zone_name = ANY(%(zone_names)s)
      AND q.corridor_name = %(corridor_name)s
),
pairs AS (
    SELECT
        t.zone_true,
        p.zone_pred,
        t.corridor_true,
        p.corridor_pred
    FROM truth t
    JOIN preds p USING (trajectory_id, zone_name, corridor_name)
)
SELECT
    COALESCE(SUM(CASE WHEN zone_true AND zone_pred THEN 1 ELSE 0 END), 0) AS zone_tp,
    COALESCE(SUM(CASE WHEN NOT zone_true AND zone_pred THEN 1 ELSE 0 END), 0) AS zone_fp,
    COALESCE(SUM(CASE WHEN zone_true AND NOT zone_pred THEN 1 ELSE 0 END), 0) AS zone_fn,
    COALESCE(SUM(CASE WHEN corridor_true AND corridor_pred THEN 1 ELSE 0 END), 0) AS corridor_tp,
    COALESCE(SUM(CASE WHEN NOT corridor_true AND corridor_pred THEN 1 ELSE 0 END), 0) AS corridor_fp,
    COALESCE(SUM(CASE WHEN corridor_true AND NOT corridor_pred THEN 1 ELSE 0 END), 0) AS corridor_fn,
    COALESCE(COUNT(*), 0) AS n_pairs
FROM pairs;
"""

    with conn.cursor() as cur:
        cur.execute(
            metric_sql,
            {
                "run_id": run_id,
                "zone_names": config.context.zone_names,
                "corridor_name": config.context.corridor_name,
                "min_overlap_m": config.queries.min_corridor_overlap_meters,
            },
        )
        row = cur.fetchone()

    keys = [
        "zone_tp",
        "zone_fp",
        "zone_fn",
        "corridor_tp",
        "corridor_fp",
        "corridor_fn",
        "n_pairs",
    ]
    return {key: int(value) for key, value in zip(keys, row, strict=True)}


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
    overwrite: bool = False,
) -> list[dict[str, float | int | str]]:
    """Run uniform/DP baselines for all requested budgets and store metrics."""
    schema = config.database.schema
    selected_methods = _parse_methods(methods, config)
    selected_budgets = _parse_budgets(budgets, config)

    selected_split = split or config.baselines.default_split
    selected_subset_name = subset_name or config.subsets.subset_name
    current_run_tag = run_tag or dt.datetime.now(dt.timezone.utc).strftime("baseline_%Y%m%dT%H%M%SZ")

    LOGGER.info(
        "Running baselines methods=%s budgets=%s split=%s subset_name=%s run_tag=%s",
        selected_methods,
        selected_budgets,
        selected_split,
        selected_subset_name,
        current_run_tag,
    )

    trajectories = _fetch_trajectories(
        conn,
        config,
        split=selected_split,
        subset_name=selected_subset_name,
    )
    LOGGER.info("Loaded %s trajectories for baseline simplification.", len(trajectories))

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
            elapsed_seconds = perf_counter() - start

            counts = _compute_run_metric_counts(conn, config, run_id)
            zone_metrics = classification_metrics(
                counts["zone_tp"],
                counts["zone_fp"],
                counts["zone_fn"],
            )
            corridor_metrics = classification_metrics(
                counts["corridor_tp"],
                counts["corridor_fp"],
                counts["corridor_fn"],
            )
            retention = _compute_retention_metrics(conn, schema, run_id)

            metric_payload: dict[str, float] = {
                "zone_entry_precision": zone_metrics["precision"],
                "zone_entry_recall": zone_metrics["recall"],
                "zone_entry_f1": zone_metrics["f1"],
                "corridor_membership_precision": corridor_metrics["precision"],
                "corridor_membership_recall": corridor_metrics["recall"],
                "corridor_membership_f1": corridor_metrics["f1"],
                "retained_point_ratio": retention["retained_point_ratio"],
                "simplification_runtime_seconds": elapsed_seconds,
                "n_query_pairs": float(counts["n_pairs"]),
                "n_simplified_trajectories": retention["trajectory_count"],
                "n_simplified_points": retention["simplified_points"],
                "n_raw_points": retention["raw_points"],
            }
            _store_metrics(conn, schema, run_id, metric_payload)

            result_row: dict[str, float | int | str] = {
                "run_id": run_id,
                "run_tag": current_run_tag,
                "method": method,
                "budget": budget,
                **metric_payload,
            }
            results.append(result_row)
            LOGGER.info(
                "Finished run_id=%s method=%s budget=%.2f zone_f1=%.4f corridor_f1=%.4f retained=%.4f",
                run_id,
                method,
                budget,
                zone_metrics["f1"],
                corridor_metrics["f1"],
                retention["retained_point_ratio"],
            )

    return results
