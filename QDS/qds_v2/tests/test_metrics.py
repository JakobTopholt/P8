"""Tests F1-based query metrics. See src/evaluation/README.md for details."""

from __future__ import annotations

import pytest
import torch

from src.evaluation.baselines import NewUniformTemporalMethod, UniformTemporalMethod
from src.evaluation.evaluate_methods import (
    _retained_point_gap_stats,
    evaluate_method,
    print_method_comparison_table,
    score_retained_mask,
)
from src.evaluation.metrics import (
    MethodEvaluation,
    clustering_f1,
    compute_average_length_loss,
    compute_length_preservation_aggregates,
    f1_score,
)
from src.simplification.simplify_trajectories import simplify_with_temporal_score_hybrid


class KeepAllMethod:
    name = "KeepAll"

    def simplify(
        self,
        points: torch.Tensor,
        boundaries: list[tuple[int, int]],
        compression_ratio: float,
    ) -> torch.Tensor:
        return torch.ones((points.shape[0],), dtype=torch.bool, device=points.device)


class DropAllMethod:
    name = "DropAll"

    def simplify(
        self,
        points: torch.Tensor,
        boundaries: list[tuple[int, int]],
        compression_ratio: float,
    ) -> torch.Tensor:
        return torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)


class FixedMaskMethod:
    def __init__(self, retained_mask: torch.Tensor) -> None:
        self.retained_mask = retained_mask
        self.name = "FixedMask"

    def simplify(
        self,
        points: torch.Tensor,
        boundaries: list[tuple[int, int]],
        compression_ratio: float,
    ) -> torch.Tensor:
        return self.retained_mask.clone()


def test_f1_score_identical_sets() -> None:
    assert f1_score({1, 2, 3}, {1, 2, 3}) == pytest.approx(1.0)


def test_f1_score_disjoint_sets() -> None:
    assert f1_score({1, 2}, {3, 4}) == pytest.approx(0.0)


def test_f1_score_both_empty() -> None:
    assert f1_score(set(), set()) == pytest.approx(1.0)


def test_f1_score_one_empty() -> None:
    assert f1_score({1}, set()) == pytest.approx(0.0)
    assert f1_score(set(), {1}) == pytest.approx(0.0)


def test_f1_score_partial_overlap() -> None:
    # precision = 2/3, recall = 2/4, F1 = 4/7.
    assert f1_score({1, 2, 3, 4}, {2, 3, 5}) == pytest.approx(4.0 / 7.0)


def test_clustering_f1_uses_co_membership_pairs() -> None:
    labels_o = [0, 0, 1, 1, -1]
    labels_s = [0, 0, 0, 1, -1]

    # original pairs: {(0, 1), (2, 3)}; simplified pairs: {(0, 1), (0, 2), (1, 2)}
    # precision = 1/3, recall = 1/2, F1 = 0.4.
    assert clustering_f1(labels_o, labels_s) == pytest.approx(0.4)


def test_clustering_f1_empty_pair_sets_agree() -> None:
    assert clustering_f1([-1, -1], [-1, -1]) == pytest.approx(1.0)


def test_evaluate_method_scores_noop_above_degenerate_baseline() -> None:
    trajectories = [
        torch.tensor([[0.0, 0.0, 0.0, 1.0], [1.0, 0.2, 0.2, 1.0]], dtype=torch.float32),
        torch.tensor([[0.0, 5.0, 5.0, 1.0], [1.0, 5.2, 5.2, 1.0]], dtype=torch.float32),
    ]
    points = torch.cat(trajectories, dim=0)
    boundaries = [(0, 2), (2, 4)]
    typed_queries = [
        {
            "type": "range",
            "params": {
                "lat_min": -1.0,
                "lat_max": 1.0,
                "lon_min": -1.0,
                "lon_max": 1.0,
                "t_start": -1.0,
                "t_end": 2.0,
            },
        }
    ]

    keep_all = evaluate_method(
        method=KeepAllMethod(),
        points=points,
        boundaries=boundaries,
        typed_queries=typed_queries,
        workload_mix={"range": 1.0},
        compression_ratio=1.0,
    )
    drop_all = evaluate_method(
        method=DropAllMethod(),
        points=points,
        boundaries=boundaries,
        typed_queries=typed_queries,
        workload_mix={"range": 1.0},
        compression_ratio=0.0,
    )

    assert keep_all.aggregate_f1 == pytest.approx(1.0)
    assert drop_all.aggregate_f1 == pytest.approx(0.0)
    assert keep_all.aggregate_f1 > drop_all.aggregate_f1


def test_score_retained_mask_matches_evaluate_method() -> None:
    trajectories = [
        torch.tensor([[0.0, 0.0, 0.0, 1.0], [1.0, 0.2, 0.2, 1.0]], dtype=torch.float32),
        torch.tensor([[0.0, 5.0, 5.0, 1.0], [1.0, 5.2, 5.2, 1.0]], dtype=torch.float32),
    ]
    points = torch.cat(trajectories, dim=0)
    boundaries = [(0, 2), (2, 4)]
    queries = [
        {
            "type": "range",
            "params": {
                "lat_min": -1.0,
                "lat_max": 1.0,
                "lon_min": -1.0,
                "lon_max": 1.0,
                "t_start": -1.0,
                "t_end": 2.0,
            },
        }
    ]
    retained = torch.tensor([True, False, True, True])

    aggregate, per_type, _, _ = score_retained_mask(
        points=points,
        boundaries=boundaries,
        retained_mask=retained,
        typed_queries=queries,
        workload_mix={"range": 1.0},
    )
    evaluated = evaluate_method(
        method=FixedMaskMethod(retained),
        points=points,
        boundaries=boundaries,
        typed_queries=queries,
        workload_mix={"range": 1.0},
        compression_ratio=0.75,
    )

    assert aggregate == pytest.approx(evaluated.aggregate_f1)
    assert per_type == pytest.approx(evaluated.per_type_f1)


def test_knn_f1_penalizes_missing_representative_points() -> None:
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],
            [1.0, 0.0, 0.1, 1.0],
            [2.0, 0.0, 0.2, 1.0],
            [3.0, 0.0, 0.3, 1.0],
            [0.0, 10.0, 10.0, 1.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 4), (4, 5)]
    queries = [
        {
            "type": "knn",
            "params": {
                "lat": 0.0,
                "lon": 0.0,
                "t_center": 1.5,
                "t_half_window": 2.0,
                "k": 1,
            },
        }
    ]
    retained = torch.tensor([True, False, False, False, True])

    aggregate, per_type, agg_combined, per_type_combined = score_retained_mask(
        points=points,
        boundaries=boundaries,
        retained_mask=retained,
        typed_queries=queries,
        workload_mix={"knn": 1.0},
    )

    # Pure answer F1: trajectory 0 still found via point 0 (closest to anchor).
    assert per_type["knn"] == pytest.approx(1.0)
    assert aggregate == pytest.approx(1.0)
    # Combined F1 still penalizes losing the kNN representative points.
    assert per_type_combined["knn"] == pytest.approx(0.4)
    assert agg_combined == pytest.approx(0.4)


def test_new_uniform_temporal_is_evenly_spaced_not_center_chunk() -> None:
    points = torch.stack(
        [torch.tensor([float(i), 0.0, float(i), 1.0], dtype=torch.float32) for i in range(10)]
    )
    boundaries = [(0, 10)]

    new_retained = NewUniformTemporalMethod().simplify(points, boundaries, compression_ratio=0.3)
    old_retained = UniformTemporalMethod().simplify(points, boundaries, compression_ratio=0.3)

    assert torch.where(new_retained)[0].tolist() == [0, 4, 9]
    assert torch.where(old_retained)[0].tolist() != [0, 4, 9]


def test_temporal_score_hybrid_keeps_base_and_score_fill() -> None:
    scores = torch.tensor([0.0, 0.0, 0.0, 0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 0.0])
    retained = simplify_with_temporal_score_hybrid(
        scores=scores,
        boundaries=[(0, 10)],
        compression_ratio=0.3,
        temporal_fraction=0.5,
        diversity_bonus=0.0,
    )

    assert torch.where(retained)[0].tolist() == [0, 5, 9]


def test_retained_point_gap_stats_measure_original_spacing() -> None:
    retained = torch.tensor([True, False, False, True, False, True, True, False, True])

    avg_gap, avg_norm_gap, max_gap = _retained_point_gap_stats(retained, boundaries=[(0, 6), (6, 9)])

    assert avg_gap == pytest.approx((3.0 + 2.0 + 2.0) / 3.0)
    assert avg_norm_gap == pytest.approx(((3.0 / 5.0) + (2.0 / 5.0) + (2.0 / 2.0)) / 3.0)
    assert max_gap == pytest.approx(3.0)


def test_length_preservation_aggregates_summarise_whole_set_with_one_value() -> None:
    """One straight (length-preserved) trajectory and one collapsed trajectory.

    Two trajectories of equal original length: traj A keeps its endpoints over a
    straight chord (ratio 1.0 since intermediate points lie on the chord); traj B
    keeps only one point so ratio collapses to 0.0. This pins the distributional
    aggregates against per-trajectory weighting expectations.
    """
    traj_a = torch.stack([torch.tensor([float(i), 0.0, float(i), 1.0]) for i in range(5)])
    traj_b = torch.stack([torch.tensor([float(i), 10.0, float(i), 1.0]) for i in range(5)])
    points = torch.cat([traj_a, traj_b], dim=0)
    boundaries = [(0, 5), (5, 10)]
    retained = torch.tensor(
        [True, False, False, False, True,
         True, False, False, False, False],
        dtype=torch.bool,
    )

    aggs = compute_length_preservation_aggregates(points, boundaries, retained)

    assert aggs["n_trajectories"] == 2
    assert aggs["mean_per_traj"] == pytest.approx(0.5)
    assert aggs["median_per_traj"] == pytest.approx(0.5)
    assert aggs["min_per_traj"] == pytest.approx(0.0)
    assert aggs["frac_above_0p9"] == pytest.approx(0.5)
    assert aggs["frac_above_0p95"] == pytest.approx(0.5)
    assert aggs["weighted_ratio"] == pytest.approx(
        compute_average_length_loss(points, boundaries, retained)
    )


def test_length_preservation_aggregates_handle_empty_eval_set() -> None:
    """All-singleton trajectories: nothing is evaluable, so default to 1.0."""
    points = torch.tensor([[0.0, 0.0, 0.0, 1.0]])
    aggs = compute_length_preservation_aggregates(points, [(0, 1)], torch.tensor([True]))

    assert aggs["n_trajectories"] == 0
    assert aggs["mean_per_traj"] == pytest.approx(1.0)
    assert aggs["median_per_traj"] == pytest.approx(1.0)


def test_method_comparison_table_labels_f1() -> None:
    table = print_method_comparison_table(
        {
            "A": MethodEvaluation(
                aggregate_f1=1.0,
                per_type_f1={"range": 1.0},
                compression_ratio=1.0,
                latency_ms=0.0,
            )
        }
    )

    assert "AnswerF1" in table
    assert "CombinedF1" in table
    assert "AvgPtGap" in table
    assert "AggregateErr" not in table


def test_method_comparison_table_shows_close_f1_values() -> None:
    table = print_method_comparison_table(
        {
            "MLQDS": MethodEvaluation(
                aggregate_f1=0.1823688112,
                per_type_f1={"range": 0.1823688112},
                compression_ratio=0.1008,
                latency_ms=0.0,
            ),
            "Random": MethodEvaluation(
                aggregate_f1=0.1824232682,
                per_type_f1={"range": 0.1824232682},
                compression_ratio=0.1008,
                latency_ms=0.0,
            ),
        }
    )

    assert "0.182369" in table
    assert "0.182423" in table
