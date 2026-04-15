"""Simplification and metric unit tests."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.metrics import classification_metrics
from src.simplification.douglas_peucker import simplify_douglas_peucker_indices
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


def test_classification_metrics() -> None:
    metrics = classification_metrics(tp=8, fp=2, fn=2)

    assert round(metrics["precision"], 4) == 0.8
    assert round(metrics["recall"], 4) == 0.8
    assert round(metrics["f1"], 4) == 0.8
