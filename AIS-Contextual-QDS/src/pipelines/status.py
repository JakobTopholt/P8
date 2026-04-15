"""Status helpers for quick table-level sanity checks."""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..db import fetch_one


def run(conn: Connection[Any], config: AppConfig) -> dict[str, int]:
    """Return key table counts for current schema."""
    schema = config.database.schema

    def count(table_name: str) -> int:
        return int(fetch_one(conn, f"SELECT COUNT(*) FROM {schema}.{table_name};") or 0)

    return {
        "study_region": count("study_region"),
        "context_zones": count("context_zones"),
        "context_corridors": count("context_corridors"),
        "trajectory_points_raw": count("trajectory_points_raw"),
        "trajectories_raw": count("trajectories_raw"),
        "trajectory_query_labels": count("trajectory_query_labels"),
        "trajectory_dev_eval_subset": count("trajectory_dev_eval_subset"),
    }
