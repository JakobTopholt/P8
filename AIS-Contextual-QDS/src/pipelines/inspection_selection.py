"""Shared run and trajectory selection helpers for inspection exports."""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..query_semantics import build_run_prediction_ctes_sql
from ..simplification import normalize_method_name


def parse_trajectory_ids(raw_ids: list[int] | None) -> list[int] | None:
    """Return ordered unique trajectory IDs, or None when no IDs were provided."""
    if not raw_ids:
        return None
    ordered: list[int] = []
    seen: set[int] = set()
    for trajectory_id in raw_ids:
        if trajectory_id not in seen:
            ordered.append(trajectory_id)
            seen.add(trajectory_id)
    return ordered


def resolve_run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_id: int | None,
    run_tag: str | None,
    method: str | None,
    budget: float | None,
) -> dict[str, object] | None:
    """Resolve one simplification run from optional inspection filters."""
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
        clauses.append("method_name = %(method)s")
        params["method"] = normalize_method_name(method)
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
            "Run benchmark first or pass a valid --run-id."
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


def select_trajectory_ids(
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
    """Select inspection trajectories, preferring query mismatches when a run is provided."""
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

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [int(row[0]) for row in cur.fetchall()]
