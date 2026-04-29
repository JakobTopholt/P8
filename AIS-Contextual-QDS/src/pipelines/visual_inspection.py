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
from ..visualization.inspection import ContextFeature, PointRecord, TrajectoryView, write_inspection_html
from .inspection_selection import parse_trajectory_ids, resolve_run, select_trajectory_ids

LOGGER = logging.getLogger(__name__)


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

    selected_ids = parse_trajectory_ids(trajectory_ids)
    run_summary = resolve_run(
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
        selected_ids = select_trajectory_ids(
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
