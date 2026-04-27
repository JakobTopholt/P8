"""Reporting exporter tests."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.reporting import write_f1_svg, write_summary_csv, write_summary_json, write_summary_markdown


def _sample_rows() -> list[dict[str, object]]:
    return [
        {
            "run_id": 1,
            "run_tag": "baseline_demo",
            "method": "uniform",
            "budget": 0.1,
            "evaluation_mode": "optimized",
            "truth_label_mode": "optimized",
            "trajectory_split": "dev",
            "subset_name": "great_belt_iter1_10days",
            "config_path": "configs/iteration1_10days.example.yaml",
            "zone_entry_precision": 0.8,
            "zone_entry_recall": 0.75,
            "zone_entry_f1": 0.774,
            "zone_entry_tp": 8.0,
            "zone_entry_fp": 2.0,
            "zone_entry_fn": 3.0,
            "zone_point_membership_macro_f1": 0.667,
            "zone_entry_event_count_macro_exact_rate": 0.833,
            "corridor_membership_precision": 0.9,
            "corridor_membership_recall": 0.7,
            "corridor_membership_f1": 0.788,
            "corridor_membership_tp": 9.0,
            "corridor_membership_fp": 1.0,
            "corridor_membership_fn": 4.0,
            "corridor_point_membership_f1": 0.714,
            "corridor_entry_event_count_exact_rate": 0.875,
            "zone_entry_zone_anchor_or_waiting_area_tp": 4.0,
            "retained_point_ratio": 0.1,
            "simplification_runtime_seconds": 0.02,
            "n_query_pairs": 500.0,
            "n_simplified_trajectories": 250.0,
            "n_simplified_points": 1000.0,
            "n_raw_points": 10000.0,
        },
        {
            "run_id": 2,
            "run_tag": "baseline_demo",
            "method": "dp",
            "budget": 0.1,
            "evaluation_mode": "segment_exact",
            "truth_label_mode": "segment_exact",
            "trajectory_split": "dev",
            "subset_name": "great_belt_iter1_10days",
            "config_path": "configs/iteration1_10days.example.yaml",
            "zone_entry_precision": 0.82,
            "zone_entry_recall": 0.76,
            "zone_entry_f1": 0.789,
            "corridor_membership_precision": 0.88,
            "corridor_membership_recall": 0.72,
            "corridor_membership_f1": 0.792,
            "retained_point_ratio": 0.1,
            "simplification_runtime_seconds": 0.06,
            "n_query_pairs": 500.0,
            "n_simplified_trajectories": 250.0,
            "n_simplified_points": 1000.0,
            "n_raw_points": 10000.0,
        },
    ]


def test_summary_exports(tmp_path: Path) -> None:
    rows = _sample_rows()

    csv_path = tmp_path / "summary.csv"
    json_path = tmp_path / "summary.json"
    markdown_path = tmp_path / "summary.md"
    svg_path = tmp_path / "summary.svg"

    write_summary_csv(rows, csv_path)
    write_summary_json(rows, json_path)
    write_summary_markdown(rows, markdown_path)
    write_f1_svg(rows, svg_path, run_tag="baseline_demo")

    assert csv_path.exists()
    assert json_path.exists()
    assert markdown_path.exists()
    assert svg_path.exists()

    csv_text = csv_path.read_text(encoding="utf-8")
    assert "zone_entry_f1" in csv_text
    assert "zone_point_membership_macro_f1" in csv_text
    assert "corridor_entry_event_count_exact_rate" in csv_text
    assert "corridor_membership_f1" in csv_text
    assert "zone_entry_zone_anchor_or_waiting_area_tp" in csv_text

    json_rows = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(json_rows) == 2
    assert json_rows[0]["run_tag"] == "baseline_demo"

    markdown_text = markdown_path.read_text(encoding="utf-8")
    assert "| method | budget | eval_mode | truth_mode |" in markdown_text
    assert "zone_point_f1" in markdown_text
    assert "zone_event_exact" in markdown_text
    assert "corridor_point_f1" in markdown_text
    assert "corridor_event_exact" in markdown_text
    assert "0.6670" in markdown_text
    assert "0.8750" in markdown_text

    svg_text = svg_path.read_text(encoding="utf-8")
    assert "Baseline Query Fidelity" in svg_text
    assert "Zone Entry F1" in svg_text
