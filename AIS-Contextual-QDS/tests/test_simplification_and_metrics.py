"""Simplification and metric unit tests."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.metrics import classification_metrics
from src.simplification.douglas_peucker import simplify_douglas_peucker_indices
from src.simplification.query_driven import B3PointEvidence, score_b3_points, simplify_b3_indices
from src.simplification.uniform import simplify_uniform_indices


def test_uniform_keeps_endpoints_and_target_size() -> None:
    indices = simplify_uniform_indices(n_points=10, target_points=4)

    assert indices[0] == 0
    assert indices[-1] == 9
    assert len(indices) == 4
    assert indices == sorted(indices)


def test_dp_keeps_endpoints_and_target_size() -> None:
    points = [(0.0, 0.0), (0.2, 0.1), (0.4, 0.5), (0.6, 0.2), (0.8, 0.7), (1.0, 1.0)]
    indices = simplify_douglas_peucker_indices(points, target_points=4)

    assert indices[0] == 0
    assert indices[-1] == len(points) - 1
    assert len(indices) == 4
    assert indices == sorted(indices)


def test_b3_keeps_query_witnesses_before_shape_only_points() -> None:
    evidence = [
        B3PointEvidence(),
        B3PointEvidence(local_turn_degrees=180.0, local_deviation_m=50.0),
        B3PointEvidence(zone_entry_segment_witnesses=1),
        B3PointEvidence(corridor_point_hit=True),
        B3PointEvidence(),
    ]

    indices = simplify_b3_indices(evidence, target_points=4)

    assert indices == [0, 2, 3, 4]


def test_b3_scores_endpoints_as_forced_anchors() -> None:
    scores = score_b3_points(
        [
            B3PointEvidence(),
            B3PointEvidence(zone_entry_segment_witnesses=1),
            B3PointEvidence(),
        ]
    )

    assert scores[0] == float("inf")
    assert scores[-1] == float("inf")
    assert scores[1] < float("inf")


def test_b3_tie_breaking_is_deterministic_under_tight_budget() -> None:
    evidence = [B3PointEvidence() for _ in range(6)]

    indices = simplify_b3_indices(evidence, target_points=4)

    assert indices == [0, 1, 2, 5]


def test_b3_keeps_temporal_guards_before_score_fill() -> None:
    evidence = [B3PointEvidence() for _ in range(10)]

    indices = simplify_b3_indices(evidence, target_points=6)

    assert indices[0] == 0
    assert indices[-1] == 9
    assert 4 in indices
    assert len(indices) == 6


def test_classification_metrics() -> None:
    metrics = classification_metrics(tp=8, fp=2, fn=2)

    assert round(metrics["precision"], 4) == 0.8
    assert round(metrics["recall"], 4) == 0.8
    assert round(metrics["f1"], 4) == 0.8
