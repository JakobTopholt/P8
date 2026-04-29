"""Simplification and metric unit tests."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.evaluation.metrics import classification_metrics
from src.simplification.douglas_peucker import simplify_douglas_peucker_indices
from src.simplification.methods import normalize_method_name, normalize_method_names
from src.simplification.query_witness import (
    QueryWitnessPointEvidence,
    score_query_witness_points,
    simplify_query_witness_indices,
)
from src.simplification.uniform import simplify_uniform_indices


def test_uniform_keeps_endpoints_and_target_size() -> None:
    indices = simplify_uniform_indices(n_points=10, target_points=4)

    assert indices[0] == 0
    assert indices[-1] == 9
    assert len(indices) == 4
    assert indices == sorted(indices)


def test_douglas_peucker_keeps_endpoints_and_target_size() -> None:
    points = [(0.0, 0.0), (0.2, 0.1), (0.4, 0.5), (0.6, 0.2), (0.8, 0.7), (1.0, 1.0)]
    indices = simplify_douglas_peucker_indices(points, target_points=4)

    assert indices[0] == 0
    assert indices[-1] == len(points) - 1
    assert len(indices) == 4
    assert indices == sorted(indices)


def test_method_name_validation_accepts_canonical_names() -> None:
    methods = normalize_method_names(["uniform", "douglas_peucker", "query_witness"])

    assert methods == ["uniform", "douglas_peucker", "query_witness"]


def test_method_name_validation_rejects_old_labels() -> None:
    for method in ["dp", "b3", "query-witness"]:
        try:
            normalize_method_name(method)
        except ValueError:
            continue
        raise AssertionError(f"Expected {method!r} to be rejected")


def test_query_witness_keeps_query_witnesses_before_shape_only_points() -> None:
    evidence = [
        QueryWitnessPointEvidence(),
        QueryWitnessPointEvidence(local_turn_degrees=180.0, local_deviation_m=50.0),
        QueryWitnessPointEvidence(zone_entry_segment_witnesses=1),
        QueryWitnessPointEvidence(corridor_point_hit=True),
        QueryWitnessPointEvidence(),
    ]

    indices = simplify_query_witness_indices(evidence, target_points=4)

    assert indices == [0, 2, 3, 4]


def test_query_witness_scores_endpoints_as_forced_anchors() -> None:
    scores = score_query_witness_points(
        [
            QueryWitnessPointEvidence(),
            QueryWitnessPointEvidence(zone_entry_segment_witnesses=1),
            QueryWitnessPointEvidence(),
        ]
    )

    assert scores[0] == float("inf")
    assert scores[-1] == float("inf")
    assert scores[1] < float("inf")


def test_query_witness_tie_breaking_is_deterministic_under_tight_budget() -> None:
    evidence = [QueryWitnessPointEvidence() for _ in range(6)]

    indices = simplify_query_witness_indices(evidence, target_points=4)

    assert indices == [0, 1, 2, 5]


def test_query_witness_keeps_temporal_guards_before_score_fill() -> None:
    evidence = [QueryWitnessPointEvidence() for _ in range(10)]

    indices = simplify_query_witness_indices(evidence, target_points=6)

    assert indices[0] == 0
    assert indices[-1] == 9
    assert 4 in indices
    assert len(indices) == 6


def test_classification_metrics() -> None:
    metrics = classification_metrics(tp=8, fp=2, fn=2)

    assert round(metrics["precision"], 4) == 0.8
    assert round(metrics["recall"], 4) == 0.8
    assert round(metrics["f1"], 4) == 0.8
