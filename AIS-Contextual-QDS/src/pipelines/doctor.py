"""Environment and pipeline preflight checks."""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import fetch_one
from ..query_semantics import QUERY_MODES
from .status import CORE_TABLES, run as status_run


def _safe_split_counts(conn: Connection[Any], schema: str) -> dict[str, int]:
    if not bool(fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": f"{schema}.trajectory_dev_eval_subset"})):
        return {}

    with conn.cursor() as cur:
        cur.execute(
            f"""
SELECT split, COUNT(*)
FROM {schema}.trajectory_dev_eval_subset
GROUP BY split
ORDER BY split;
"""
        )
        return {str(split_name): int(count_value) for split_name, count_value in cur.fetchall()}


def _label_mode_summary(
    conn: Connection[Any],
    config: AppConfig,
    *,
    trajectory_count: int,
) -> dict[str, dict[str, object]]:
    schema = config.database.schema
    labels_table = f"{schema}.trajectory_query_labels"
    if not bool(fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": labels_table})):
        return {
            mode: {
                "rows": 0,
                "expected_rows": trajectory_count * len(config.context.zone_names),
                "complete": False,
                "latest_computed_at": None,
            }
            for mode in sorted(QUERY_MODES)
        }

    expected_rows = trajectory_count * len(config.context.zone_names)
    rows_by_mode: dict[str, tuple[int, str | None]] = {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
SELECT label_mode, COUNT(*) AS n_rows, MAX(computed_at)::text
FROM {schema}.trajectory_query_labels
WHERE corridor_name = %(corridor_name)s
GROUP BY label_mode
ORDER BY label_mode;
""",
            {"corridor_name": config.context.corridor_name},
        )
        for label_mode, n_rows, latest_computed_at in cur.fetchall():
            rows_by_mode[str(label_mode)] = (int(n_rows), str(latest_computed_at) if latest_computed_at else None)

    return {
        mode: {
            "rows": rows_by_mode.get(mode, (0, None))[0],
            "expected_rows": expected_rows,
            "complete": expected_rows > 0 and rows_by_mode.get(mode, (0, None))[0] == expected_rows,
            "latest_computed_at": rows_by_mode.get(mode, (0, None))[1],
        }
        for mode in sorted(QUERY_MODES)
    }


def run(conn: Connection[Any], config: AppConfig) -> dict[str, object]:
    """Run lightweight environment and dataset readiness checks."""
    schema = config.database.schema
    source_table = config.database.source_points_table
    status_payload = status_run(conn, config)

    source_exists = bool(fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": source_table}))
    estimated_source_rows = 0
    if source_exists:
        estimated_source_rows = int(
            fetch_one(
                conn,
                "SELECT COALESCE(reltuples, 0)::bigint FROM pg_class WHERE oid = to_regclass(%(table)s);",
                {"table": source_table},
            )
            or 0
        )

    trajectory_count = int(status_payload.get("trajectories_raw", 0))
    point_count = int(status_payload.get("trajectory_points_raw", 0))
    split_counts = _safe_split_counts(conn, schema)
    label_modes = _label_mode_summary(conn, config, trajectory_count=trajectory_count)

    context_ready = (
        int(status_payload.get("study_region", 0)) >= 1
        and int(status_payload.get("context_zones", 0)) == len(config.context.zone_names)
        and int(status_payload.get("context_corridors", 0)) == 1
    )
    trajectories_ready = trajectory_count > 0 and point_count > 0
    subset_ready = (
        split_counts.get("dev", 0) == config.subsets.dev_size
        and split_counts.get("eval", 0) == config.subsets.eval_size
    )

    with conn.cursor() as cur:
        cur.execute("SHOW config_file;")
        config_file = str(cur.fetchone()[0])
        cur.execute("SELECT name FROM pg_settings WHERE pending_restart = TRUE ORDER BY name;")
        pending_restart = [str(row[0]) for row in cur.fetchall()]

    warnings: list[str] = []
    if not source_exists:
        warnings.append(f"Source table is missing: {source_table}")
    if not status_payload.get("schema_exists", False):
        warnings.append(f"Project schema is missing: {schema}. Run bootstrap first.")
    elif status_payload.get("missing_tables"):
        warnings.append(
            "Project schema is only partially bootstrapped. Missing tables: "
            + ", ".join(str(name) for name in status_payload["missing_tables"])
        )
    if not context_ready:
        warnings.append("Context layers are incomplete. Load the study region, 3 zones, and 1 corridor.")
    if not trajectories_ready:
        warnings.append("Raw trajectories are not ready. Run prepare-data or build-trajectories.")
    if not subset_ready:
        warnings.append("Dev/eval subset is missing or incomplete for the configured iteration.")
    for mode_name, details in label_modes.items():
        if not bool(details["complete"]):
            warnings.append(
                f"Truth labels for label_mode={mode_name!r} are missing or incomplete. "
                f"Expected {details['expected_rows']} rows, found {details['rows']}."
            )
    if pending_restart:
        warnings.append(
            "PostgreSQL has settings pending restart: " + ", ".join(pending_restart)
        )

    return {
        "config": {
            "config_name": config.project.name,
            "schema": schema,
            "source_points_table": source_table,
            "default_label_mode": config.performance.label_mode,
            "default_evaluation_mode": config.performance.evaluation_mode,
            "session_profile": config.performance.session_profile,
        },
        "source": {
            "exists": source_exists,
            "estimated_rows": estimated_source_rows,
        },
        "schema": {
            "exists": bool(status_payload.get("schema_exists", False)),
            "missing_tables": status_payload.get("missing_tables", []),
        },
        "context": {
            "study_region": int(status_payload.get("study_region", 0)),
            "zones": int(status_payload.get("context_zones", 0)),
            "corridors": int(status_payload.get("context_corridors", 0)),
            "ready": context_ready,
        },
        "trajectories": {
            "trajectory_points_raw": point_count,
            "trajectories_raw": trajectory_count,
            "ready": trajectories_ready,
        },
        "labels": {
            "modes": label_modes,
        },
        "subsets": {
            "split_counts": split_counts,
            "ready": subset_ready,
        },
        "postgres": {
            "config_file": config_file,
            "pending_restart": pending_restart,
        },
        "ready": {
            "for_prepare_data": source_exists and bool(status_payload.get("schema_exists", False)) and context_ready,
            "for_benchmark": trajectories_ready and subset_ready and bool(
                label_modes.get(config.performance.label_mode, {}).get("complete", False)
            ),
            "for_benchmark_by_truth_mode": {
                mode: trajectories_ready and subset_ready and bool(details["complete"])
                for mode, details in label_modes.items()
            },
        },
        "warnings": warnings,
    }
