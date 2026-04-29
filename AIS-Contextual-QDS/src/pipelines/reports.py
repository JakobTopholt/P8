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
from ..simplification import normalize_method_names

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
        params["methods"] = normalize_method_names(methods)

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
    m.metric_key,
    m.metric_value
FROM {schema}.simplification_runs r
LEFT JOIN {schema}.benchmark_metrics m ON m.run_id = r.run_id
WHERE r.run_tag = %(run_tag)s
{method_filter_sql}
ORDER BY r.method_name, r.budget_ratio, r.run_id, m.metric_key;
"""

    rows_by_run_id: dict[int, dict[str, float | int | str]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for (
            run_id,
            run_tag_value,
            method,
            budget,
            evaluation_mode,
            truth_label_mode,
            trajectory_split,
            subset_name,
            config_path,
            metric_key,
            metric_value,
        ) in cur.fetchall():
            normalized_run_id = int(run_id)
            row = rows_by_run_id.setdefault(
                normalized_run_id,
                {
                    "run_id": normalized_run_id,
                    "run_tag": str(run_tag_value),
                    "method": str(method),
                    "budget": float(budget),
                    "evaluation_mode": str(evaluation_mode),
                    "truth_label_mode": str(truth_label_mode),
                    "trajectory_split": str(trajectory_split),
                    "subset_name": str(subset_name),
                    "config_path": str(config_path) if config_path is not None else "",
                },
            )
            if metric_key is not None:
                row[str(metric_key)] = float(metric_value) if metric_value is not None else 0.0

    return sorted(rows_by_run_id.values(), key=lambda row: (str(row["method"]), float(row["budget"]), int(row["run_id"])))


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
