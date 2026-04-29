"""Export visual inspection HTML for raw/simplified trajectory comparisons."""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..paths import resolve_project_path
from ..query_semantics import build_run_prediction_ctes_sql, normalize_query_mode
from ..simplification import expand_method_filter
from ..visualization.inspection import ContextFeature, PointRecord, TrajectoryView, write_inspection_html

LOGGER = logging.getLogger(__name__)


def _parse_trajectory_ids(raw_ids: list[int] | None) -> list[int] | None:
    if not raw_ids:
        return None
    ordered: list[int] = []
    seen: set[int] = set()
    for trajectory_id in raw_ids:
        if trajectory_id not in seen:
            ordered.append(trajectory_id)
            seen.add(trajectory_id)
    return ordered


def _resolve_run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_id: int | None,
    run_tag: str | None,
    method: str | None,
    budget: float | None,
) -> dict[str, object] | None:
    schema = config.database.schema
    if run_id is None and run_tag is None and method is None and budget is None:
        return None

    clauses: list[str] = []
    params: dict[str, object] = {}
    if run_id is not None:
        clauses.append("run_id = %(run_id)s")
        params["run_id"] = run_id
    if run_tag is not None:
        clauses.append("run_tag = %(run_tag)s")
        params["run_tag"] = run_tag
    if method is not None:
        clauses.append("method_name = ANY(%(methods)s)")
        params["methods"] = expand_method_filter([method])
    if budget is not None:
        clauses.append("budget_ratio = %(budget)s")
        params["budget"] = budget

    where_sql = " AND ".join(clauses) if clauses else "TRUE"
    sql = f"""
SELECT
    run_id,
    run_tag,
    method_name,
    budget_ratio,
    started_at,
    evaluation_mode,
    truth_label_mode,
    trajectory_split,
    subset_name,
    config_path
FROM {schema}.simplification_runs
WHERE {where_sql}
ORDER BY started_at DESC, run_id DESC
LIMIT 1;
"""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(
            "No simplification run matched the requested filters. "
            "Run baselines first or pass a valid --run-id."
        )

    return {
        "run_id": int(row[0]),
        "run_tag": str(row[1]),
        "method_name": str(row[2]),
        "budget_ratio": float(row[3]),
        "started_at": str(row[4]),
        "evaluation_mode": str(row[5]),
        "truth_label_mode": str(row[6]),
        "trajectory_split": str(row[7]),
        "subset_name": str(row[8]),
        "config_path": str(row[9]) if row[9] is not None else "",
    }


def _fetch_context_features(conn: Connection[Any], config: AppConfig) -> list[ContextFeature]:
    schema = config.database.schema
    features: list[ContextFeature] = []
    queries = [
        (
            f"SELECT region_name, ST_AsGeoJSON(geom) FROM {schema}.study_region WHERE is_active = TRUE;",
            "study_region",
        ),
        (
            f"SELECT zone_name, ST_AsGeoJSON(geom) FROM {schema}.context_zones ORDER BY zone_name;",
            "zone",
        ),
        (
            f"SELECT corridor_name, ST_AsGeoJSON(geom) FROM {schema}.context_corridors ORDER BY corridor_name;",
            "corridor",
        ),
    ]
    with conn.cursor() as cur:
        for sql, kind in queries:
            cur.execute(sql)
            for name, geometry_json in cur.fetchall():
                features.append(ContextFeature(name=str(name), kind=kind, geometry=json.loads(geometry_json)))
    return features


def _select_trajectory_ids(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_id: int | None,
    split: str,
    subset_name: str,
    limit: int,
    evaluation_mode: str,
    truth_label_mode: str,
) -> list[int]:
    schema = config.database.schema
    if split not in {"all", "dev", "eval"}:
        raise ValueError("split must be one of all/dev/eval.")

    subset_join = ""
    subset_where = ""
    params: dict[str, object] = {
        "subset_name": subset_name,
        "split": split,
        "limit": limit,
        "corridor_name": config.context.corridor_name,
        "truth_label_mode": truth_label_mode,
    }
    if split != "all":
        subset_join = (
            f"JOIN {schema}.trajectory_dev_eval_subset s "
            "ON s.trajectory_id = t.trajectory_id"
        )
        subset_where = "AND s.subset_name = %(subset_name)s AND s.split = %(split)s"

    if run_id is None:
        sql = f"""
WITH truth AS (
    SELECT
        t.trajectory_id,
        BOOL_OR(q.zone_entry) AS any_zone_entry,
        BOOL_OR(q.corridor_membership) AS corridor_membership,
        t.n_points
    FROM {schema}.trajectories_raw t
    {subset_join}
    LEFT JOIN {schema}.trajectory_query_labels q
      ON q.trajectory_id = t.trajectory_id
     AND q.corridor_name = %(corridor_name)s
     AND q.label_mode = %(truth_label_mode)s
    WHERE TRUE {subset_where}
    GROUP BY t.trajectory_id, t.n_points
)
SELECT trajectory_id
FROM truth
ORDER BY any_zone_entry DESC NULLS LAST, corridor_membership DESC NULLS LAST, n_points DESC, trajectory_id
LIMIT %(limit)s;
"""
    else:
        params["run_id"] = run_id
        params["zone_names"] = config.context.zone_names
        params["min_overlap_m"] = config.queries.min_corridor_overlap_meters
        prediction_ctes = build_run_prediction_ctes_sql(
            schema,
            mode=evaluation_mode,
            run_points_where_sql="WHERE run_id = %(run_id)s",
        )
        sql = f"""
WITH {prediction_ctes},
truth AS (
    SELECT
        t.trajectory_id,
        q.zone_name,
        q.corridor_name,
        q.zone_entry,
        q.corridor_membership,
        t.n_points
    FROM {schema}.trajectories_raw t
    JOIN run_lines rl ON rl.trajectory_id = t.trajectory_id
    {subset_join}
    JOIN {schema}.trajectory_query_labels q
      ON q.trajectory_id = t.trajectory_id
     AND q.corridor_name = %(corridor_name)s
     AND q.label_mode = %(truth_label_mode)s
    WHERE TRUE {subset_where}
),
pairs AS (
    SELECT
        truth.trajectory_id,
        truth.zone_name,
        truth.zone_entry,
        truth.corridor_membership,
        truth.n_points,
        preds.zone_entry_pred,
        preds.corridor_pred
    FROM truth
    JOIN preds
      ON preds.trajectory_id = truth.trajectory_id
     AND preds.zone_name = truth.zone_name
     AND preds.corridor_name = truth.corridor_name
),
scored AS (
    SELECT
        trajectory_id,
        BOOL_OR(zone_entry) AS any_zone_entry,
        BOOL_OR(corridor_membership) AS corridor_membership,
        MAX(n_points) AS n_points,
        SUM(CASE WHEN zone_entry <> zone_entry_pred THEN 1 ELSE 0 END)
            + MAX(CASE WHEN corridor_membership <> corridor_pred THEN 1 ELSE 0 END) AS query_mismatches
    FROM pairs
    GROUP BY trajectory_id
)
SELECT trajectory_id
FROM scored
ORDER BY query_mismatches DESC, any_zone_entry DESC, corridor_membership DESC, n_points DESC, trajectory_id
LIMIT %(limit)s;
"""
        params["zone_names"] = config.context.zone_names
        params["min_overlap_m"] = config.queries.min_corridor_overlap_meters
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [int(row[0]) for row in cur.fetchall()]


def _fetch_raw_points(
    conn: Connection[Any],
    config: AppConfig,
    trajectory_ids: list[int],
) -> dict[int, list[PointRecord]]:
    schema = config.database.schema
    sql = f"""
SELECT trajectory_id, point_seq, ts, lon, lat
FROM {schema}.trajectory_points_raw
WHERE trajectory_id = ANY(%(trajectory_ids)s)
ORDER BY trajectory_id, point_seq;
"""
    grouped: dict[int, list[PointRecord]] = {trajectory_id: [] for trajectory_id in trajectory_ids}
    with conn.cursor() as cur:
        cur.execute(sql, {"trajectory_ids": trajectory_ids})
        for trajectory_id, point_seq, ts, lon, lat in cur.fetchall():
            grouped.setdefault(int(trajectory_id), []).append(
                PointRecord(lon=float(lon), lat=float(lat), seq=int(point_seq), ts=str(ts))
            )
    return grouped


def _fetch_simplified_points(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_id: int,
    trajectory_ids: list[int],
) -> dict[int, list[PointRecord]]:
    schema = config.database.schema
    sql = f"""
SELECT trajectory_id, point_seq, source_point_seq, ts, lon, lat
FROM {schema}.trajectories_simplified_points
WHERE run_id = %(run_id)s
  AND trajectory_id = ANY(%(trajectory_ids)s)
ORDER BY trajectory_id, point_seq;
"""
    grouped: dict[int, list[PointRecord]] = {trajectory_id: [] for trajectory_id in trajectory_ids}
    with conn.cursor() as cur:
        cur.execute(sql, {"run_id": run_id, "trajectory_ids": trajectory_ids})
        for trajectory_id, point_seq, source_point_seq, ts, lon, lat in cur.fetchall():
            grouped.setdefault(int(trajectory_id), []).append(
                PointRecord(
                    lon=float(lon),
                    lat=float(lat),
                    seq=int(point_seq),
                    ts=str(ts),
                    source_seq=int(source_point_seq) if source_point_seq is not None else None,
                )
            )
    return grouped


def _fetch_truth(
    conn: Connection[Any],
    config: AppConfig,
    trajectory_ids: list[int],
    *,
    truth_label_mode: str,
) -> tuple[dict[int, dict[str, bool]], dict[int, bool]]:
    schema = config.database.schema
    sql = f"""
SELECT trajectory_id, zone_name, zone_entry, corridor_membership
FROM {schema}.trajectory_query_labels
WHERE trajectory_id = ANY(%(trajectory_ids)s)
  AND corridor_name = %(corridor_name)s
  AND label_mode = %(truth_label_mode)s
ORDER BY trajectory_id, zone_name;
"""
    zone_truth: dict[int, dict[str, bool]] = {trajectory_id: {} for trajectory_id in trajectory_ids}
    corridor_truth: dict[int, bool] = {trajectory_id: False for trajectory_id in trajectory_ids}
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "trajectory_ids": trajectory_ids,
                "corridor_name": config.context.corridor_name,
                "truth_label_mode": truth_label_mode,
            },
        )
        for trajectory_id, zone_name, zone_entry, corridor_membership in cur.fetchall():
            tid = int(trajectory_id)
            zone_truth.setdefault(tid, {})[str(zone_name)] = bool(zone_entry)
            corridor_truth[tid] = bool(corridor_membership)
    return zone_truth, corridor_truth


def _fetch_predictions(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_id: int,
    trajectory_ids: list[int],
    evaluation_mode: str,
) -> tuple[dict[int, dict[str, bool]], dict[int, bool]]:
    schema = config.database.schema
    prediction_ctes = build_run_prediction_ctes_sql(
        schema,
        mode=evaluation_mode,
        run_points_where_sql="WHERE run_id = %(run_id)s AND trajectory_id = ANY(%(trajectory_ids)s)",
    )
    sql = f"""
WITH {prediction_ctes}
SELECT
    trajectory_id,
    zone_name,
    zone_entry_pred,
    corridor_pred
FROM preds
ORDER BY trajectory_id, zone_name;
"""
    zone_pred: dict[int, dict[str, bool]] = {trajectory_id: {} for trajectory_id in trajectory_ids}
    corridor_pred: dict[int, bool] = {trajectory_id: False for trajectory_id in trajectory_ids}
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "run_id": run_id,
                "trajectory_ids": trajectory_ids,
                "zone_names": config.context.zone_names,
                "corridor_name": config.context.corridor_name,
                "min_overlap_m": config.queries.min_corridor_overlap_meters,
            },
        )
        for trajectory_id, zone_name, zone_entry_pred, corridor_value in cur.fetchall():
            tid = int(trajectory_id)
            zone_pred.setdefault(tid, {})[str(zone_name)] = bool(zone_entry_pred)
            corridor_pred[tid] = bool(corridor_value)
    return zone_pred, corridor_pred


def _default_output_path(run_summary: dict[str, object] | None) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if not run_summary:
        return resolve_project_path(f"results/figures/inspection_raw_{stamp}.html")
    method = str(run_summary["method_name"]).replace(" ", "_")
    budget = str(run_summary["budget_ratio"]).replace(".", "p")
    return resolve_project_path(f"results/figures/inspection_{method}_{budget}_{stamp}.html")


def run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    output_path: Path | None = None,
    run_id: int | None = None,
    run_tag: str | None = None,
    method: str | None = None,
    budget: float | None = None,
    split: str | None = None,
    subset_name: str | None = None,
    trajectory_ids: list[int] | None = None,
    limit: int = 12,
    max_points_per_line: int = 1500,
    evaluation_mode: str | None = None,
    truth_label_mode: str | None = None,
) -> dict[str, object]:
    """Export visual inspection HTML."""
    if limit <= 0:
        raise ValueError("limit must be > 0.")

    selected_ids = _parse_trajectory_ids(trajectory_ids)
    run_summary = _resolve_run(
        conn,
        config,
        run_id=run_id,
        run_tag=run_tag,
        method=method,
        budget=budget,
    )
    resolved_run_id = int(run_summary["run_id"]) if run_summary else None
    selected_split = split or (
        str(run_summary["trajectory_split"])
        if run_summary and str(run_summary.get("trajectory_split", ""))
        else "dev"
    )
    selected_subset_name = subset_name or (
        str(run_summary["subset_name"])
        if run_summary and str(run_summary.get("subset_name", ""))
        else config.subsets.subset_name
    )
    resolved_evaluation_mode = normalize_query_mode(
        evaluation_mode,
        default=(
            str(run_summary["evaluation_mode"])
            if run_summary and evaluation_mode is None
            else config.performance.evaluation_mode
        ),
    )
    resolved_truth_label_mode = normalize_query_mode(
        truth_label_mode,
        default=(
            str(run_summary["truth_label_mode"])
            if run_summary and truth_label_mode is None
            else config.performance.label_mode
        ),
    )

    context_features = _fetch_context_features(conn, config)
    if not context_features:
        raise RuntimeError("No context features found. Run load-context first.")

    if selected_ids is None:
        selected_ids = _select_trajectory_ids(
            conn,
            config,
            run_id=resolved_run_id,
            split=selected_split,
            subset_name=selected_subset_name,
            limit=limit,
            evaluation_mode=resolved_evaluation_mode,
            truth_label_mode=resolved_truth_label_mode,
        )
    else:
        selected_ids = selected_ids[:limit]

    if not selected_ids:
        raise RuntimeError("No trajectories selected for visual inspection.")

    raw_points = _fetch_raw_points(conn, config, selected_ids)
    simplified_points: dict[int, list[PointRecord]] = {trajectory_id: [] for trajectory_id in selected_ids}
    if resolved_run_id is not None:
        simplified_points = _fetch_simplified_points(
            conn,
            config,
            run_id=resolved_run_id,
            trajectory_ids=selected_ids,
        )

    zone_truth, corridor_truth = _fetch_truth(
        conn,
        config,
        selected_ids,
        truth_label_mode=resolved_truth_label_mode,
    )
    zone_pred: dict[int, dict[str, bool]] = {trajectory_id: {} for trajectory_id in selected_ids}
    corridor_pred: dict[int, bool] = {}
    if resolved_run_id is not None:
        zone_pred, corridor_pred = _fetch_predictions(
            conn,
            config,
            run_id=resolved_run_id,
            trajectory_ids=selected_ids,
            evaluation_mode=resolved_evaluation_mode,
        )

    render_run_summary = None if run_summary is None else {
        **run_summary,
        "evaluation_mode": resolved_evaluation_mode,
        "truth_label_mode": resolved_truth_label_mode,
    }
    trajectories = [
        TrajectoryView(
            trajectory_id=trajectory_id,
            raw_points=raw_points.get(trajectory_id, []),
            simplified_points=simplified_points.get(trajectory_id, []),
            zone_truth=zone_truth.get(trajectory_id, {}),
            zone_pred=zone_pred.get(trajectory_id, {}),
            corridor_truth=corridor_truth.get(trajectory_id),
            corridor_pred=corridor_pred.get(trajectory_id),
        )
        for trajectory_id in selected_ids
        if raw_points.get(trajectory_id)
    ]

    if not trajectories:
        raise RuntimeError("Selected trajectories had no raw points.")

    final_output_path = output_path or _default_output_path(run_summary)
    title = "AIS-QDS Visual Inspection"
    written_path = write_inspection_html(
        final_output_path,
        title=title,
        config=config,
        context_features=context_features,
        trajectories=trajectories,
        run_summary=render_run_summary,
        max_points_per_line=max_points_per_line,
    )

    LOGGER.info("Visual inspection HTML written to %s", written_path)
    return {
        "output_path": str(written_path),
        "trajectory_count": len(trajectories),
        "trajectory_ids": [trajectory.trajectory_id for trajectory in trajectories],
        "run": render_run_summary,
        "evaluation_mode": resolved_evaluation_mode,
        "truth_label_mode": resolved_truth_label_mode,
        "split": selected_split,
        "subset_name": selected_subset_name,
    }
