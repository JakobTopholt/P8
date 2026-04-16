"""Visual inspection renderer tests."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.visualization.inspection import ContextFeature, PointRecord, TrajectoryView, render_inspection_html


def test_render_inspection_html_includes_context_and_query_status() -> None:
    config = load_config(PROJECT_ROOT / "configs" / "mvp.example.yaml")
    context = [
        ContextFeature(
            name="great_belt_study_area",
            kind="study_region",
            geometry={
                "type": "Polygon",
                "coordinates": [[[10.0, 55.0], [11.0, 55.0], [11.0, 56.0], [10.0, 56.0], [10.0, 55.0]]],
            },
        ),
        ContextFeature(
            name="zone_port_approach",
            kind="zone",
            geometry={
                "type": "Polygon",
                "coordinates": [[[10.2, 55.2], [10.4, 55.2], [10.4, 55.4], [10.2, 55.4], [10.2, 55.2]]],
            },
        ),
        ContextFeature(
            name="corridor_main_transit_lane",
            kind="corridor",
            geometry={
                "type": "LineString",
                "coordinates": [[10.1, 55.1], [10.8, 55.8]],
            },
        ),
    ]
    trajectory = TrajectoryView(
        trajectory_id=7,
        raw_points=[
            PointRecord(lon=10.1, lat=55.1, seq=1, ts="2026-01-01T00:00:00+00:00"),
            PointRecord(lon=10.5, lat=55.5, seq=2, ts="2026-01-01T00:10:00+00:00"),
            PointRecord(lon=10.8, lat=55.8, seq=3, ts="2026-01-01T00:20:00+00:00"),
        ],
        simplified_points=[
            PointRecord(lon=10.1, lat=55.1, seq=1, ts="2026-01-01T00:00:00+00:00", source_seq=1),
            PointRecord(lon=10.8, lat=55.8, seq=2, ts="2026-01-01T00:20:00+00:00", source_seq=3),
        ],
        zone_truth={"zone_port_approach": True},
        zone_pred={"zone_port_approach": False},
        corridor_truth=True,
        corridor_pred=True,
    )

    html = render_inspection_html(
        title="Demo",
        config=config,
        context_features=context,
        trajectories=[trajectory],
        run_summary={"run_id": 4, "method_name": "uniform", "budget_ratio": 0.1},
    )

    assert "Trajectory 7" in html
    assert "query mismatch" in html
    assert "zone_port_approach" in html
    assert "corridor_main_transit_lane" in html
    assert "<svg class='map'" in html
    assert "raw trajectory" in html
    assert "simplified trajectory" in html
