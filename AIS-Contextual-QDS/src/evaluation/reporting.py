"""Reporting helpers for simplification benchmark outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path

SUMMARY_COLUMNS = [
    "run_id",
    "run_tag",
    "method",
    "budget",
    "evaluation_mode",
    "truth_label_mode",
    "trajectory_split",
    "subset_name",
    "config_path",
    "zone_entry_precision",
    "zone_entry_recall",
    "zone_entry_f1",
    "zone_entry_tp",
    "zone_entry_fp",
    "zone_entry_fn",
    "zone_entry_tn",
    "zone_entry_support",
    "zone_entry_true_positive",
    "zone_entry_predicted_positive",
    "zone_entry_macro_f1",
    "zone_point_membership_precision",
    "zone_point_membership_recall",
    "zone_point_membership_f1",
    "zone_point_membership_macro_precision",
    "zone_point_membership_macro_recall",
    "zone_point_membership_macro_f1",
    "zone_entry_event_count_exact_rate",
    "zone_entry_event_count_macro_exact_rate",
    "zone_entry_event_count_mae",
    "zone_entry_event_count_macro_mae",
    "corridor_membership_precision",
    "corridor_membership_recall",
    "corridor_membership_f1",
    "corridor_membership_tp",
    "corridor_membership_fp",
    "corridor_membership_fn",
    "corridor_membership_tn",
    "corridor_membership_support",
    "corridor_membership_true_positive",
    "corridor_membership_predicted_positive",
    "corridor_point_membership_precision",
    "corridor_point_membership_recall",
    "corridor_point_membership_f1",
    "corridor_entry_event_count_exact_rate",
    "corridor_entry_event_count_mae",
    "retained_point_ratio",
    "simplification_runtime_seconds",
    "n_query_pairs",
    "n_corridor_trajectories",
    "n_simplified_trajectories",
    "n_simplified_points",
    "n_simplified_segments",
    "n_raw_points",
]

_COLOR_PALETTE = [
    "#1f77b4",
    "#d62728",
    "#2ca02c",
    "#9467bd",
    "#ff7f0e",
    "#17becf",
    "#8c564b",
    "#e377c2",
]


def _xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def write_summary_csv(rows: list[dict[str, object]], output_path: Path) -> None:
    """Write summary rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    extra_columns = sorted(
        {
            key
            for row in rows
            for key in row
            if key not in SUMMARY_COLUMNS
        }
    )
    fieldnames = SUMMARY_COLUMNS + extra_columns
    with output_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_summary_json(rows: list[dict[str, object]], output_path: Path) -> None:
    """Write summary rows to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file_obj:
        json.dump(rows, file_obj, indent=2, sort_keys=True)


def write_summary_markdown(rows: list[dict[str, object]], output_path: Path) -> None:
    """Write a compact markdown summary table."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "method",
        "budget",
        "eval_mode",
        "truth_mode",
        "zone_entry_f1",
        "zone_tp/fp/fn",
        "zone_point_f1",
        "zone_event_exact",
        "corridor_membership_f1",
        "corridor_tp/fp/fn",
        "corridor_point_f1",
        "corridor_event_exact",
        "retained_point_ratio",
        "runtime_s",
    ]

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    for row in rows:
        values = [
            str(row.get("method", "")),
            f"{float(row.get('budget', 0.0)):.3f}",
            str(row.get("evaluation_mode", "")),
            str(row.get("truth_label_mode", "")),
            f"{float(row.get('zone_entry_f1', 0.0)):.4f}",
            (
                f"{int(float(row.get('zone_entry_tp', 0.0)))}/"
                f"{int(float(row.get('zone_entry_fp', 0.0)))}/"
                f"{int(float(row.get('zone_entry_fn', 0.0)))}"
            ),
            f"{float(row.get('zone_point_membership_macro_f1', row.get('zone_point_membership_f1', 0.0))):.4f}",
            f"{float(row.get('zone_entry_event_count_macro_exact_rate', row.get('zone_entry_event_count_exact_rate', 0.0))):.4f}",
            f"{float(row.get('corridor_membership_f1', 0.0)):.4f}",
            (
                f"{int(float(row.get('corridor_membership_tp', 0.0)))}/"
                f"{int(float(row.get('corridor_membership_fp', 0.0)))}/"
                f"{int(float(row.get('corridor_membership_fn', 0.0)))}"
            ),
            f"{float(row.get('corridor_point_membership_f1', 0.0)):.4f}",
            f"{float(row.get('corridor_entry_event_count_exact_rate', 0.0)):.4f}",
            f"{float(row.get('retained_point_ratio', 0.0)):.4f}",
            f"{float(row.get('simplification_runtime_seconds', 0.0)):.3f}",
        ]
        lines.append("| " + " | ".join(values) + " |")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _line_points(
    method_rows: list[dict[str, object]],
    budgets: list[float],
    *,
    x_left: float,
    x_right: float,
    y_top: float,
    y_bottom: float,
    metric_key: str,
) -> list[tuple[float, float]]:
    budget_to_row = {float(row["budget"]): row for row in method_rows}

    points: list[tuple[float, float]] = []
    for budget in budgets:
        if budget not in budget_to_row:
            continue

        x = x_left + ((budget - budgets[0]) / max(budgets[-1] - budgets[0], 1e-12)) * (x_right - x_left)
        y_val = float(budget_to_row[budget].get(metric_key, 0.0))
        y_val = min(max(y_val, 0.0), 1.0)
        y = y_bottom - y_val * (y_bottom - y_top)
        points.append((x, y))

    return points


def _render_metric_panel(
    svg_parts: list[str],
    rows: list[dict[str, object]],
    *,
    methods: list[str],
    method_colors: dict[str, str],
    budgets: list[float],
    panel_x: float,
    panel_y: float,
    panel_w: float,
    panel_h: float,
    metric_key: str,
    panel_title: str,
) -> None:
    x_left = panel_x + 52
    x_right = panel_x + panel_w - 20
    y_top = panel_y + 26
    y_bottom = panel_y + panel_h - 34

    svg_parts.append(
        f'<rect x="{panel_x:.1f}" y="{panel_y:.1f}" width="{panel_w:.1f}" height="{panel_h:.1f}" fill="#ffffff" stroke="#d9d9d9" />'
    )
    svg_parts.append(
        f'<text x="{panel_x + 10:.1f}" y="{panel_y + 18:.1f}" font-size="13" fill="#202020">{_xml_escape(panel_title)}</text>'
    )

    for i in range(6):
        value = i * 0.2
        y = y_bottom - value * (y_bottom - y_top)
        svg_parts.append(
            f'<line x1="{x_left:.1f}" y1="{y:.1f}" x2="{x_right:.1f}" y2="{y:.1f}" stroke="#efefef" stroke-width="1" />'
        )
        svg_parts.append(
            f'<text x="{x_left - 8:.1f}" y="{y + 4:.1f}" text-anchor="end" font-size="11" fill="#666666">{value:.1f}</text>'
        )

    svg_parts.append(
        f'<line x1="{x_left:.1f}" y1="{y_bottom:.1f}" x2="{x_right:.1f}" y2="{y_bottom:.1f}" stroke="#999999" stroke-width="1.2" />'
    )
    svg_parts.append(
        f'<line x1="{x_left:.1f}" y1="{y_top:.1f}" x2="{x_left:.1f}" y2="{y_bottom:.1f}" stroke="#999999" stroke-width="1.2" />'
    )

    for budget in budgets:
        x = x_left + ((budget - budgets[0]) / max(budgets[-1] - budgets[0], 1e-12)) * (x_right - x_left)
        svg_parts.append(
            f'<line x1="{x:.1f}" y1="{y_bottom:.1f}" x2="{x:.1f}" y2="{y_bottom + 5:.1f}" stroke="#999999" stroke-width="1" />'
        )
        svg_parts.append(
            f'<text x="{x:.1f}" y="{y_bottom + 18:.1f}" text-anchor="middle" font-size="11" fill="#666666">{int(round(budget * 100)):d}%</text>'
        )

    for method in methods:
        method_rows = [row for row in rows if str(row.get("method")) == method]
        points = _line_points(
            method_rows,
            budgets,
            x_left=x_left,
            x_right=x_right,
            y_top=y_top,
            y_bottom=y_bottom,
            metric_key=metric_key,
        )
        if len(points) < 2:
            continue

        points_attr = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        color = method_colors[method]
        svg_parts.append(
            f'<polyline points="{points_attr}" fill="none" stroke="{color}" stroke-width="2" />'
        )
        for x, y in points:
            svg_parts.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.5" fill="{color}" />'
            )


def write_f1_svg(rows: list[dict[str, object]], output_path: Path, *, run_tag: str) -> None:
    """Render an SVG plot for zone/corridor F1 over budgets."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        output_path.write_text(
            "<svg xmlns='http://www.w3.org/2000/svg' width='960' height='120'>"
            "<text x='16' y='64' font-size='14'>No rows to plot.</text></svg>",
            encoding="utf-8",
        )
        return

    budgets = sorted({float(row["budget"]) for row in rows})
    methods = sorted({str(row["method"]) for row in rows})
    method_colors = {
        method: _COLOR_PALETTE[idx % len(_COLOR_PALETTE)]
        for idx, method in enumerate(methods)
    }

    width = 980
    height = 640

    svg_parts: list[str] = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>",
        "<rect x='0' y='0' width='100%' height='100%' fill='#fafafa' />",
        (
            f"<text x='18' y='28' font-size='16' font-weight='600' fill='#1f1f1f'>"
            f"Benchmark Query Fidelity - run_tag={_xml_escape(run_tag)}</text>"
        ),
    ]

    panel_x = 20
    panel_w = width - 40
    panel_h = 250
    _render_metric_panel(
        svg_parts,
        rows,
        methods=methods,
        method_colors=method_colors,
        budgets=budgets,
        panel_x=panel_x,
        panel_y=48,
        panel_w=panel_w,
        panel_h=panel_h,
        metric_key="zone_entry_f1",
        panel_title="Zone Entry F1",
    )
    _render_metric_panel(
        svg_parts,
        rows,
        methods=methods,
        method_colors=method_colors,
        budgets=budgets,
        panel_x=panel_x,
        panel_y=332,
        panel_w=panel_w,
        panel_h=panel_h,
        metric_key="corridor_membership_f1",
        panel_title="Corridor Membership F1",
    )

    legend_x = width - 230
    legend_y = 22
    for idx, method in enumerate(methods):
        y = legend_y + idx * 18
        color = method_colors[method]
        svg_parts.append(f"<line x1='{legend_x}' y1='{y}' x2='{legend_x + 20}' y2='{y}' stroke='{color}' stroke-width='3' />")
        svg_parts.append(
            f"<text x='{legend_x + 26}' y='{y + 4}' font-size='12' fill='#333333'>{_xml_escape(method)}</text>"
        )

    svg_parts.append("</svg>")
    output_path.write_text("\n".join(svg_parts), encoding="utf-8")
