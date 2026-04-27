"""Summary report generation for baseline benchmark runs."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from psycopg import Connection

from ..config import AppConfig
from ..evaluation.reporting import write_f1_svg, write_summary_csv, write_summary_json, write_summary_markdown
from ..paths import resolve_project_path

LOGGER = logging.getLogger(__name__)


def _sanitize_tag(tag: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", tag)


def _resolve_run_tag(conn: Connection[Any], schema: str, run_tag: str | None) -> str:
    if run_tag:
        return run_tag

    sql = f"SELECT run_tag FROM {schema}.simplification_runs ORDER BY started_at DESC LIMIT 1;"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    if row is None:
        raise RuntimeError("No simplification runs found. Run baselines first.")
    return str(row[0])


def _fetch_rows(
    conn: Connection[Any],
    schema: str,
    *,
    run_tag: str,
    methods: list[str] | None,
) -> list[dict[str, float | int | str]]:
    method_filter_sql = ""
    params: dict[str, object] = {"run_tag": run_tag}
    if methods:
        method_filter_sql = "  AND r.method_name = ANY(%(methods)s)\n"
        params["methods"] = methods

    sql = f"""
SELECT
    r.run_id,
    r.run_tag,
    r.method_name AS method,
    r.budget_ratio AS budget,
    r.evaluation_mode,
    r.truth_label_mode,
    r.trajectory_split,
    r.subset_name,
    r.config_path,
    MAX(CASE WHEN m.metric_key = 'zone_entry_precision' THEN m.metric_value END) AS zone_entry_precision,
    MAX(CASE WHEN m.metric_key = 'zone_entry_recall' THEN m.metric_value END) AS zone_entry_recall,
    MAX(CASE WHEN m.metric_key = 'zone_entry_f1' THEN m.metric_value END) AS zone_entry_f1,
    MAX(CASE WHEN m.metric_key = 'corridor_membership_precision' THEN m.metric_value END) AS corridor_membership_precision,
    MAX(CASE WHEN m.metric_key = 'corridor_membership_recall' THEN m.metric_value END) AS corridor_membership_recall,
    MAX(CASE WHEN m.metric_key = 'corridor_membership_f1' THEN m.metric_value END) AS corridor_membership_f1,
    MAX(CASE WHEN m.metric_key = 'retained_point_ratio' THEN m.metric_value END) AS retained_point_ratio,
    MAX(CASE WHEN m.metric_key = 'simplification_runtime_seconds' THEN m.metric_value END) AS simplification_runtime_seconds,
    MAX(CASE WHEN m.metric_key = 'n_query_pairs' THEN m.metric_value END) AS n_query_pairs,
    MAX(CASE WHEN m.metric_key = 'n_simplified_trajectories' THEN m.metric_value END) AS n_simplified_trajectories,
    MAX(CASE WHEN m.metric_key = 'n_simplified_points' THEN m.metric_value END) AS n_simplified_points,
    MAX(CASE WHEN m.metric_key = 'n_raw_points' THEN m.metric_value END) AS n_raw_points
FROM {schema}.simplification_runs r
LEFT JOIN {schema}.benchmark_metrics m ON m.run_id = r.run_id
WHERE r.run_tag = %(run_tag)s
{method_filter_sql}
GROUP BY r.run_id, r.run_tag, r.method_name, r.budget_ratio
ORDER BY r.method_name, r.budget_ratio;
"""

    rows: list[dict[str, float | int | str]] = []
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [desc.name for desc in cur.description]
        for record in cur.fetchall():
            row = dict(zip(columns, record, strict=True))
            row["run_id"] = int(row["run_id"])
            row["run_tag"] = str(row["run_tag"])
            row["method"] = str(row["method"])
            row["evaluation_mode"] = str(row["evaluation_mode"])
            row["truth_label_mode"] = str(row["truth_label_mode"])
            row["trajectory_split"] = str(row["trajectory_split"])
            row["subset_name"] = str(row["subset_name"])
            row["config_path"] = str(row["config_path"]) if row["config_path"] is not None else ""
            for key in columns:
                if key in {
                    "run_id",
                    "run_tag",
                    "method",
                    "evaluation_mode",
                    "truth_label_mode",
                    "trajectory_split",
                    "subset_name",
                    "config_path",
                }:
                    continue
                value = row.get(key)
                row[key] = float(value) if value is not None else 0.0
            rows.append(row)

    return rows


def run(
    conn: Connection[Any],
    config: AppConfig,
    *,
    run_tag: str | None = None,
    methods: list[str] | None = None,
) -> dict[str, object]:
    """Generate CSV/JSON/Markdown summary and F1 SVG plot for a baseline run_tag."""
    schema = config.database.schema
    resolved_run_tag = _resolve_run_tag(conn, schema, run_tag)

    rows = _fetch_rows(conn, schema, run_tag=resolved_run_tag, methods=methods)
    if not rows:
        raise RuntimeError(
            f"No benchmark rows found for run_tag={resolved_run_tag!r}. "
            "Run baselines first or check filters."
        )

    safe_tag = _sanitize_tag(resolved_run_tag)
    metrics_dir = resolve_project_path(config.paths.metrics_dir)
    figures_dir = resolve_project_path(config.paths.figures_dir)

    csv_path = metrics_dir / f"baseline_summary_{safe_tag}.csv"
    json_path = metrics_dir / f"baseline_summary_{safe_tag}.json"
    markdown_path = metrics_dir / f"baseline_summary_{safe_tag}.md"
    plot_path = figures_dir / f"baseline_f1_{safe_tag}.svg"

    write_summary_csv(rows, csv_path)
    write_summary_json(rows, json_path)
    write_summary_markdown(rows, markdown_path)
    write_f1_svg(rows, plot_path, run_tag=resolved_run_tag)

    LOGGER.info(
        "Baseline summary exported for run_tag=%s (%s rows).",
        resolved_run_tag,
        len(rows),
    )

    return {
        "run_tag": resolved_run_tag,
        "rows": len(rows),
        "csv_path": str(csv_path.resolve()),
        "json_path": str(json_path.resolve()),
        "markdown_path": str(markdown_path.resolve()),
        "plot_path": str(plot_path.resolve()),
        "methods": sorted({str(row["method"]) for row in rows}),
        "budgets": sorted({float(row["budget"]) for row in rows}),
        "evaluation_modes": sorted({str(row["evaluation_mode"]) for row in rows}),
        "truth_label_modes": sorted({str(row["truth_label_mode"]) for row in rows}),
    }
