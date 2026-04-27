"""Status helpers for quick table-level sanity checks."""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import fetch_one

CORE_TABLES = [
    "study_region",
    "context_zones",
    "context_corridors",
    "trajectory_points_raw",
    "trajectories_raw",
    "trajectory_query_labels",
    "trajectory_point_context_features",
    "trajectory_dev_eval_subset",
]


def _schema_exists(conn: Connection[Any], schema: str) -> bool:
    return bool(fetch_one(conn, "SELECT to_regnamespace(%(schema)s) IS NOT NULL;", {"schema": schema}))


def _table_exists(conn: Connection[Any], schema: str, table_name: str) -> bool:
    return bool(
        fetch_one(conn, "SELECT to_regclass(%(table)s) IS NOT NULL;", {"table": f"{schema}.{table_name}"})
    )


def run(conn: Connection[Any], config: AppConfig) -> dict[str, object]:
    """Return key table counts for current schema without assuming bootstrap already ran."""
    schema = config.database.schema
    schema_exists = _schema_exists(conn, schema)
    if not schema_exists:
        return {
            "schema_exists": False,
            "missing_tables": CORE_TABLES,
            "trajectory_query_labels_by_mode": {},
        }

    missing_tables = [table_name for table_name in CORE_TABLES if not _table_exists(conn, schema, table_name)]

    def count(table_name: str) -> int:
        if table_name in missing_tables:
            return 0
        return int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.{table_name};") or 0)

    label_counts_by_mode: dict[str, int] = {}
    if "trajectory_query_labels" not in missing_tables and bool(
        fetch_one(
            conn,
            (
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = %(schema)s AND table_name = 'trajectory_query_labels' AND column_name = 'label_mode'"
                ");"
            ),
            {"schema": schema},
        )
    ):
        with conn.cursor() as cur:
            cur.execute(
                f"""
SELECT label_mode, COUNT(*)
FROM {schema}.trajectory_query_labels
GROUP BY label_mode
ORDER BY label_mode;
"""
            )
            label_counts_by_mode = {str(label_mode): int(count_value) for label_mode, count_value in cur.fetchall()}

    return {
        "schema_exists": True,
        "missing_tables": missing_tables,
        "study_region": count("study_region"),
        "context_zones": count("context_zones"),
        "context_corridors": count("context_corridors"),
        "trajectory_points_raw": count("trajectory_points_raw"),
        "trajectories_raw": count("trajectories_raw"),
        "trajectory_query_labels": count("trajectory_query_labels"),
        "trajectory_query_labels_by_mode": label_counts_by_mode,
        "trajectory_point_context_features": count("trajectory_point_context_features"),
        "trajectory_dev_eval_subset": count("trajectory_dev_eval_subset"),
    }
