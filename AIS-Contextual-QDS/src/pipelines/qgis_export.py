"""Export selected AIS-QDS inspection layers for QGIS."""

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
from ..visualization.qgis_package import make_feature, sanitize_layer_name, write_feature_collection, write_qgis_project
from .visual_inspection import _parse_trajectory_ids, _resolve_run, _select_trajectory_ids

LOGGER = logging.getLogger(__name__)


def _json_geometry(raw_value: str) -> dict[str, Any]:
    return json.loads(raw_value)


def _bool_props(prefix: str, values: dict[str, bool]) -> dict[str, bool]:
    return {f"{prefix}_{sanitize_layer_name(key)}": value for key, value in values.items()}


def _fetch_truth_by_trajectory(
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
    zone_truth = {trajectory_id: {} for trajectory_id in trajectory_ids}
    corridor_truth = {trajectory_id: False for trajectory_id in trajectory_ids}
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


def _fetch_predictions_by_trajectory(
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
    zone_pred = {trajectory_id: {} for trajectory_id in trajectory_ids}
    corridor_pred = {trajectory_id: False for trajectory_id in trajectory_ids}
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


def _fetch_context_features(conn: Connection[Any], config: AppConfig) -> dict[str, list[dict[str, Any]]]:
    schema = config.database.schema
    outputs: dict[str, list[dict[str, Any]]] = {
        "study_region": [],
        "context_zones": [],
        "context_corridor": [],
    }
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT region_name, is_active, ST_AsGeoJSON(geom) FROM {schema}.study_region ORDER BY is_active DESC, region_name;"
        )
        for region_name, is_active, geometry_json in cur.fetchall():
            outputs["study_region"].append(
                make_feature(
                    _json_geometry(geometry_json),
                    {
                        "name": str(region_name),
                        "kind": "study_region",
                        "is_active": bool(is_active),
                    },
                )
            )

        cur.execute(f"SELECT zone_name, ST_AsGeoJSON(geom) FROM {schema}.context_zones ORDER BY zone_name;")
        for zone_name, geometry_json in cur.fetchall():
            outputs["context_zones"].append(
                make_feature(
                    _json_geometry(geometry_json),
                    {
                        "name": str(zone_name),
                        "kind": "zone",
                    },
                )
            )

        cur.execute(f"SELECT corridor_name, ST_AsGeoJSON(geom) FROM {schema}.context_corridors ORDER BY corridor_name;")
        for corridor_name, geometry_json in cur.fetchall():
            outputs["context_corridor"].append(
                make_feature(
                    _json_geometry(geometry_json),
                    {
                        "name": str(corridor_name),
                        "kind": "corridor",
                    },
                )
            )
    return outputs


def _fetch_raw_trajectory_features(
    conn: Connection[Any],
    config: AppConfig,
    trajectory_ids: list[int],
    *,
    zone_truth: dict[int, dict[str, bool]],
    corridor_truth: dict[int, bool],
) -> list[dict[str, Any]]:
    schema = config.database.schema
    sql = f"""
SELECT trajectory_id, mmsi, start_ts, end_ts, n_points, ST_AsGeoJSON(geom)
FROM {schema}.trajectories_raw
WHERE trajectory_id = ANY(%(trajectory_ids)s)
ORDER BY trajectory_id;
"""
    features: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql, {"trajectory_ids": trajectory_ids})
        for trajectory_id, mmsi, start_ts, end_ts, n_points, geometry_json in cur.fetchall():
            tid = int(trajectory_id)
            properties: dict[str, Any] = {
                "trajectory_id": tid,
                "mmsi": int(mmsi),
                "start_ts": str(start_ts),
                "end_ts": str(end_ts),
                "raw_points": int(n_points),
                "truth_corridor_membership": corridor_truth.get(tid, False),
            }
            properties.update(_bool_props("truth", zone_truth.get(tid, {})))
            features.append(make_feature(_json_geometry(geometry_json), properties))
    return features


def _fetch_raw_point_features(
    conn: Connection[Any],
    config: AppConfig,
    trajectory_ids: list[int],
) -> list[dict[str, Any]]:
    schema = config.database.schema
    sql = f"""
SELECT trajectory_id, point_seq, mmsi, ts, sog, cog, nav_status, ST_AsGeoJSON(geom)
FROM {schema}.trajectory_points_raw
WHERE trajectory_id = ANY(%(trajectory_ids)s)
ORDER BY trajectory_id, point_seq;
"""
    features: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql, {"trajectory_ids": trajectory_ids})
        for trajectory_id, point_seq, mmsi, ts, sog, cog, nav_status, geometry_json in cur.fetchall():
            features.append(
                make_feature(
                    _json_geometry(geometry_json),
                    {
                        "trajectory_id": int(trajectory_id),
                        "point_seq": int(point_seq),
                        "mmsi": int(mmsi),
                        "ts": str(ts),
                        "sog": float(sog) if sog is not None else None,
                        "cog": float(cog) if cog is not None else None,
                        "nav_status": str(nav_status) if nav_status is not None else None,
                    },
                )
            )
    return features


def _fetch_simplified_trajectory_features(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_summary: dict[str, object],
    trajectory_ids: list[int],
    zone_truth: dict[int, dict[str, bool]],
    corridor_truth: dict[int, bool],
    zone_pred: dict[int, dict[str, bool]],
    corridor_pred: dict[int, bool],
) -> list[dict[str, Any]]:
    schema = config.database.schema
    sql = f"""
WITH simplified AS (
    SELECT
        trajectory_id,
        COUNT(*) AS simplified_points,
        ST_MakeLine(geom ORDER BY point_seq) AS geom
    FROM {schema}.trajectories_simplified_points
    WHERE run_id = %(run_id)s
      AND trajectory_id = ANY(%(trajectory_ids)s)
    GROUP BY trajectory_id
)
SELECT
    s.trajectory_id,
    t.mmsi,
    t.n_points AS raw_points,
    s.simplified_points,
    ST_AsGeoJSON(s.geom)
FROM simplified s
JOIN {schema}.trajectories_raw t USING (trajectory_id)
ORDER BY s.trajectory_id;
"""
    features: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "run_id": int(run_summary["run_id"]),
                "trajectory_ids": trajectory_ids,
            },
        )
        for trajectory_id, mmsi, raw_points, simplified_points, geometry_json in cur.fetchall():
            tid = int(trajectory_id)
            truth_zones = zone_truth.get(tid, {})
            pred_zones = zone_pred.get(tid, {})
            zone_mismatches = sum(
                1
                for zone_name, truth_value in truth_zones.items()
                if zone_name in pred_zones and pred_zones[zone_name] != truth_value
            )
            corridor_mismatch = corridor_pred.get(tid) != corridor_truth.get(tid) if tid in corridor_pred else False
            properties: dict[str, Any] = {
                "trajectory_id": tid,
                "mmsi": int(mmsi),
                "run_id": int(run_summary["run_id"]),
                "run_tag": str(run_summary["run_tag"]),
                "method_name": str(run_summary["method_name"]),
                "budget_ratio": float(run_summary["budget_ratio"]),
                "raw_points": int(raw_points),
                "simplified_points": int(simplified_points),
                "retained_ratio": float(simplified_points) / float(raw_points) if raw_points else None,
                "truth_corridor_membership": corridor_truth.get(tid, False),
                "pred_corridor_membership": corridor_pred.get(tid),
                "corridor_mismatch": corridor_mismatch,
                "zone_mismatch_count": zone_mismatches,
                "query_mismatch_count": zone_mismatches + int(corridor_mismatch),
            }
            properties.update(_bool_props("truth", truth_zones))
            properties.update(_bool_props("pred", pred_zones))
            features.append(make_feature(_json_geometry(geometry_json), properties))
    return features


def _fetch_simplified_point_features(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_summary: dict[str, object],
    trajectory_ids: list[int],
) -> list[dict[str, Any]]:
    schema = config.database.schema
    sql = f"""
SELECT trajectory_id, point_seq, source_point_seq, mmsi, ts, ST_AsGeoJSON(geom)
FROM {schema}.trajectories_simplified_points
WHERE run_id = %(run_id)s
  AND trajectory_id = ANY(%(trajectory_ids)s)
ORDER BY trajectory_id, point_seq;
"""
    features: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "run_id": int(run_summary["run_id"]),
                "trajectory_ids": trajectory_ids,
            },
        )
        for trajectory_id, point_seq, source_point_seq, mmsi, ts, geometry_json in cur.fetchall():
            features.append(
                make_feature(
                    _json_geometry(geometry_json),
                    {
                        "trajectory_id": int(trajectory_id),
                        "point_seq": int(point_seq),
                        "source_point_seq": int(source_point_seq) if source_point_seq is not None else None,
                        "mmsi": int(mmsi),
                        "ts": str(ts),
                        "run_id": int(run_summary["run_id"]),
                        "method_name": str(run_summary["method_name"]),
                        "budget_ratio": float(run_summary["budget_ratio"]),
                    },
                )
            )
    return features


def _default_output_dir(run_summary: dict[str, object] | None) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if run_summary is None:
        return resolve_project_path(f"results/figures/qgis_inspection_raw_{stamp}")
    method = sanitize_layer_name(str(run_summary["method_name"]))
    budget = str(run_summary["budget_ratio"]).replace(".", "p")
    return resolve_project_path(f"results/figures/qgis_inspection_{method}_{budget}_{stamp}")


def _write_manifest(
    output_dir: Path,
    *,
    config: AppConfig,
    run_summary: dict[str, object] | None,
    evaluation_mode: str,
    truth_label_mode: str,
    trajectory_ids: list[int],
    layer_files: list[dict[str, str]],
) -> Path:
    manifest_path = output_dir / "README.md"
    run_text = "raw-only" if run_summary is None else (
        f"run_id={run_summary['run_id']}, method={run_summary['method_name']}, "
        f"budget={run_summary['budget_ratio']}, run_tag={run_summary['run_tag']}"
    )
    layer_lines = "\n".join(f"- `{layer['path']}`: {layer['name']}" for layer in layer_files)
    manifest_path.write_text(
        "\n".join(
            [
                "# AIS-QDS QGIS Inspection Package",
                "",
                f"Config: `{config.project.name}`",
                f"Run: {run_text}",
                f"Evaluation mode: `{evaluation_mode}`",
                f"Truth label mode: `{truth_label_mode}`",
                f"Trajectory IDs: {', '.join(str(value) for value in trajectory_ids)}",
                "",
                "Open `ais_qds_inspection.qgs` in QGIS, or add the GeoJSON layers manually.",
                "",
                "## Layers",
                "",
                layer_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )
    return manifest_path.resolve()


def run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    output_dir: Path | None = None,
    run_id: int | None = None,
    run_tag: str | None = None,
    method: str | None = None,
    budget: float | None = None,
    split: str | None = None,
    subset_name: str | None = None,
    trajectory_ids: list[int] | None = None,
    limit: int = 12,
    include_points: bool = True,
    evaluation_mode: str | None = None,
    truth_label_mode: str | None = None,
) -> dict[str, object]:
    """Export a QGIS-ready inspection package."""
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
        raise RuntimeError("No trajectories selected for QGIS export.")

    final_output_dir = output_dir or _default_output_dir(run_summary)
    final_output_dir = resolve_project_path(final_output_dir)
    final_output_dir.mkdir(parents=True, exist_ok=True)

    layer_files: list[dict[str, str]] = []

    def write_layer(filename: str, name: str, geometry_type: str, features: list[dict[str, Any]]) -> None:
        path = write_feature_collection(final_output_dir / filename, name=name, features=features)
        layer_files.append(
            {
                "name": name,
                "path": path.name,
                "geometry_type": geometry_type,
                "feature_count": str(len(features)),
            }
        )

    context_layers = _fetch_context_features(conn, config)
    write_layer("study_region.geojson", "study_region", "Polygon", context_layers["study_region"])
    write_layer("context_zones.geojson", "context_zones", "Polygon", context_layers["context_zones"])
    write_layer("context_corridor.geojson", "context_corridor", "Polygon", context_layers["context_corridor"])

    zone_truth, corridor_truth = _fetch_truth_by_trajectory(
        conn,
        config,
        selected_ids,
        truth_label_mode=resolved_truth_label_mode,
    )
    write_layer(
        "raw_trajectories.geojson",
        "raw_trajectories",
        "LineString",
        _fetch_raw_trajectory_features(
            conn,
            config,
            selected_ids,
            zone_truth=zone_truth,
            corridor_truth=corridor_truth,
        ),
    )

    if include_points:
        write_layer(
            "raw_points.geojson",
            "raw_points",
            "Point",
            _fetch_raw_point_features(conn, config, selected_ids),
        )

    if run_summary is not None:
        zone_pred, corridor_pred = _fetch_predictions_by_trajectory(
            conn,
            config,
            run_id=int(run_summary["run_id"]),
            trajectory_ids=selected_ids,
            evaluation_mode=resolved_evaluation_mode,
        )
        write_layer(
            "simplified_trajectories.geojson",
            "simplified_trajectories",
            "LineString",
            _fetch_simplified_trajectory_features(
                conn,
                config,
                run_summary=run_summary,
                trajectory_ids=selected_ids,
                zone_truth=zone_truth,
                corridor_truth=corridor_truth,
                zone_pred=zone_pred,
                corridor_pred=corridor_pred,
            ),
        )
        if include_points:
            write_layer(
                "simplified_points.geojson",
                "simplified_points",
                "Point",
                _fetch_simplified_point_features(
                    conn,
                    config,
                    run_summary=run_summary,
                    trajectory_ids=selected_ids,
                ),
            )

    qgis_project_path = write_qgis_project(
        final_output_dir / "ais_qds_inspection.qgs",
        project_name="AIS-QDS Inspection",
        layers=layer_files,
    )
    manifest_path = _write_manifest(
        final_output_dir,
        config=config,
        run_summary=run_summary,
        evaluation_mode=resolved_evaluation_mode,
        truth_label_mode=resolved_truth_label_mode,
        trajectory_ids=selected_ids,
        layer_files=layer_files,
    )

    LOGGER.info("QGIS inspection package written to %s", final_output_dir)
    return {
        "output_dir": str(final_output_dir.resolve()),
        "qgis_project_path": str(qgis_project_path),
        "manifest_path": str(manifest_path),
        "trajectory_count": len(selected_ids),
        "trajectory_ids": selected_ids,
        "layers": layer_files,
        "run": run_summary,
        "evaluation_mode": resolved_evaluation_mode,
        "truth_label_mode": resolved_truth_label_mode,
        "split": selected_split,
        "subset_name": selected_subset_name,
    }
