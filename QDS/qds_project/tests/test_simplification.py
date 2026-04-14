"""Tests for AIS trajectory simplification."""
import pytest
import torch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.simplification.simplify_trajectories import (
    simplify_trajectories,
    _infer_trajectory_boundaries,
    _enforce_trajectory_constraints,
    _simplify_per_trajectory,
)
from src.models.trajectory_qds_model import TrajectoryQDSModel
from src.models.turn_aware_qds_model import TurnAwareQDSModel


class _FixedBaselineModel(TrajectoryQDSModel):
    """Deterministic model used to force fallback-path behavior in tests."""

    def __init__(self, fixed_scores: torch.Tensor):
        super().__init__()
        self._fixed_scores = fixed_scores

    def forward(self, points, queries):
        n = points.shape[0]
        fs = self._fixed_scores
        if fs.numel() == 1:
            return fs.to(points.device).expand(n)
        if fs.shape[0] == n:
            return fs.to(points.device)
        if fs.shape[0] > n:
            return fs[:n].to(points.device)
        repeats = (n + fs.shape[0] - 1) // fs.shape[0]
        return fs.repeat(repeats)[:n].to(points.device)

def _make_points_and_queries(n=50):
    torch.manual_seed(0)
    pts = torch.zeros(n, 8)
    pts[:, 0] = torch.arange(n).float()
    pts[:, 1] = 50.0 + torch.rand(n)
    pts[:, 2] = 0.0 + torch.rand(n)
    pts[:, 3] = torch.rand(n) * 20.0
    pts[:, 4] = torch.rand(n) * 360.0
    # is_start / is_end endpoint flags
    pts[0, 5] = 1.0
    pts[-1, 6] = 1.0
    # turn_score column (already in [0, 1]; endpoints = 0)
    pts[1:-1, 7] = torch.rand(n - 2)
    queries = torch.tensor([
        [50.0, 51.0, 0.0, 1.0, 0.0, 25.0],
        [50.2, 50.8, 0.2, 0.8, 10.0, 40.0],
    ])
    return pts, queries

class TestSimplifyTrajectories:
    def test_returns_three_values(self):
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        result = simplify_trajectories(pts, model, queries, threshold=0.5)
        assert len(result) == 3

    def test_output_columns(self):
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(pts, model, queries, threshold=0.5)
        assert simplified.shape[1] == 8

    def test_threshold_zero_keeps_all_global_mode(self):
        """In global threshold mode, threshold=0 retains all points."""
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(
            pts, model, queries, threshold=0.0, compression_ratio=None
        )
        assert simplified.shape[0] == pts.shape[0]

    def test_threshold_one_falls_back_global_mode(self):
        """In global threshold mode, threshold=1 falls back to at least 1 point."""
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(
            pts, model, queries, threshold=1.0, compression_ratio=None
        )
        assert simplified.shape[0] >= 1

    def test_non_empty_output(self):
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(pts, model, queries, threshold=0.5)
        assert simplified.shape[0] >= 1

    def test_mask_shape(self):
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(pts, model, queries, threshold=0.5)
        assert mask.shape == (pts.shape[0],)
        assert mask.dtype == torch.bool

    def test_scores_shape(self):
        pts, queries = _make_points_and_queries()
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(pts, model, queries, threshold=0.5)
        assert scores.shape == (pts.shape[0],)

    def test_endpoints_always_retained(self):
        """First and last point of each trajectory must always be retained."""
        pts, queries = _make_points_and_queries(n=20)
        model = TrajectoryQDSModel()
        _, mask, _ = simplify_trajectories(pts, model, queries, threshold=0.99)
        assert mask[0].item(),  "First point must be retained"
        assert mask[-1].item(), "Last point must be retained"

    def test_min_points_per_trajectory(self):
        """At least min_points_per_trajectory points must be kept per trajectory."""
        pts, queries = _make_points_and_queries(n=20)
        model = TrajectoryQDSModel()
        boundaries = [(0, 20)]
        _, mask, _ = simplify_trajectories(
            pts, model, queries, threshold=0.99,
            trajectory_boundaries=boundaries,
            min_points_per_trajectory=3,
        )
        assert mask.sum().item() >= 3

    def test_multi_trajectory_endpoints_retained(self):
        """Endpoints of every trajectory in a multi-trajectory batch must be kept."""
        torch.manual_seed(1)
        # Two trajectories: points 0–9, points 10–19
        pts = torch.zeros(20, 8)
        pts[:, 0] = torch.arange(20).float()
        pts[:, 1] = 50.0 + torch.rand(20)
        pts[:, 2] = torch.rand(20)
        pts[:, 3] = torch.rand(20) * 20.0
        pts[0, 5] = 1.0
        pts[9, 6] = 1.0
        pts[10, 5] = 1.0
        pts[19, 6] = 1.0
        queries = torch.tensor([[50.0, 51.0, 0.0, 1.0, 0.0, 20.0]])
        model = TrajectoryQDSModel()
        boundaries = [(0, 10), (10, 20)]
        _, mask, _ = simplify_trajectories(
            pts, model, queries, threshold=0.99,
            trajectory_boundaries=boundaries,
        )
        assert mask[0].item(),  "Start of traj 1 must be retained"
        assert mask[9].item(),  "End of traj 1 must be retained"
        assert mask[10].item(), "Start of traj 2 must be retained"
        assert mask[19].item(), "End of traj 2 must be retained"

    # ------------------------------------------------------------------
    # Per-trajectory compression mode tests
    # ------------------------------------------------------------------

    def test_per_traj_all_trajectories_retained(self):
        """Per-trajectory mode must keep at least one point from every trajectory."""
        torch.manual_seed(2)
        # 3 separate trajectories of 20 points each
        pts = torch.zeros(60, 8)
        pts[:, 0] = torch.arange(60).float()
        pts[:, 1] = 50.0 + torch.rand(60)
        pts[:, 2] = torch.rand(60)
        pts[:, 3] = torch.rand(60) * 20.0
        pts[0, 5] = 1.0; pts[19, 6] = 1.0
        pts[20, 5] = 1.0; pts[39, 6] = 1.0
        pts[40, 5] = 1.0; pts[59, 6] = 1.0
        queries = torch.tensor([[50.0, 51.0, 0.0, 1.0, 0.0, 60.0]])
        model = TrajectoryQDSModel()
        boundaries = [(0, 20), (20, 40), (40, 60)]
        _, mask, _ = simplify_trajectories(
            pts, model, queries,
            trajectory_boundaries=boundaries,
            compression_ratio=0.2,
            min_points_per_trajectory=2,
        )
        for start, end in boundaries:
            assert mask[start:end].any().item(), \
                f"Trajectory [{start}, {end}) must have at least one retained point"

    def test_per_traj_respects_budget(self):
        """Per-trajectory mode should keep ~compression_ratio * traj_len points."""
        torch.manual_seed(3)
        n = 50
        pts = torch.zeros(n, 8)
        pts[:, 0] = torch.arange(n).float()
        pts[:, 1] = 50.0 + torch.rand(n)
        pts[:, 2] = torch.rand(n)
        pts[:, 3] = torch.rand(n) * 20.0
        pts[0, 5] = 1.0
        pts[-1, 6] = 1.0
        queries = torch.tensor([[50.0, 51.0, 0.0, 1.0, 0.0, 50.0]])
        model = TrajectoryQDSModel()
        ratio = 0.4
        min_pts = 3
        expected = max(min_pts, int(ratio * n))
        _, mask, _ = simplify_trajectories(
            pts, model, queries,
            trajectory_boundaries=[(0, n)],
            compression_ratio=ratio,
            min_points_per_trajectory=min_pts,
        )
        assert mask.sum().item() == expected, \
            f"Expected {expected} retained, got {mask.sum().item()}"

    def test_per_traj_endpoints_retained(self):
        """Per-trajectory mode must always retain trajectory endpoints."""
        pts, queries = _make_points_and_queries(n=30)
        model = TrajectoryQDSModel()
        _, mask, _ = simplify_trajectories(
            pts, model, queries,
            trajectory_boundaries=[(0, 30)],
            compression_ratio=0.1,
            min_points_per_trajectory=2,
        )
        assert mask[0].item(),  "Start endpoint must be retained"
        assert mask[29].item(), "End endpoint must be retained"

    def test_per_traj_min_floor_applied(self):
        """When compression_ratio*traj_len < min_points, min_points is used."""
        pts, queries = _make_points_and_queries(n=10)
        model = TrajectoryQDSModel()
        _, mask, _ = simplify_trajectories(
            pts, model, queries,
            trajectory_boundaries=[(0, 10)],
            compression_ratio=0.1,   # 0.1 * 10 = 1 → below min
            min_points_per_trajectory=5,
        )
        assert mask.sum().item() >= 5


class TestInferTrajectoryBoundaries:
    def test_single_trajectory(self):
        pts = torch.zeros(10, 8)
        pts[0, 5] = 1.0
        boundaries = _infer_trajectory_boundaries(pts)
        assert boundaries == [(0, 10)]

    def test_two_trajectories(self):
        pts = torch.zeros(20, 8)
        pts[0, 5] = 1.0
        pts[10, 5] = 1.0
        boundaries = _infer_trajectory_boundaries(pts)
        assert boundaries == [(0, 10), (10, 20)]

    def test_no_flags_fallback(self):
        """No is_start flags → treat entire tensor as one trajectory."""
        pts = torch.zeros(15, 8)
        boundaries = _infer_trajectory_boundaries(pts)
        assert boundaries == [(0, 15)]

    def test_five_feature_points_fallback(self):
        """Points with fewer than 6 features → single boundary."""
        pts = torch.zeros(10, 5)
        boundaries = _infer_trajectory_boundaries(pts)
        assert boundaries == [(0, 10)]


class TestEnforceTrajectoryConstraints:
    def test_endpoints_forced(self):
        mask = torch.zeros(10, dtype=torch.bool)
        scores = torch.rand(10)
        _enforce_trajectory_constraints(mask, scores, [(0, 10)], min_points_per_trajectory=1)
        assert mask[0].item() and mask[9].item(), "First and last points must both be retained"

    def test_min_points_satisfied(self):
        mask = torch.zeros(10, dtype=torch.bool)
        scores = torch.arange(10, dtype=torch.float32)
        _enforce_trajectory_constraints(mask, scores, [(0, 10)], min_points_per_trajectory=5)
        assert mask.sum().item() >= 5

    def test_short_trajectory(self):
        """A 2-point trajectory: min_points=3 should keep both available points."""
        mask = torch.zeros(2, dtype=torch.bool)
        scores = torch.tensor([0.8, 0.2])
        _enforce_trajectory_constraints(mask, scores, [(0, 2)], min_points_per_trajectory=3)
        assert mask.sum().item() == 2


class TestSimplifyPerTrajectory:
    """Unit tests for the _simplify_per_trajectory helper."""

    def test_budget_applied(self):
        """The correct number of points is selected per trajectory."""
        mask = torch.zeros(20, dtype=torch.bool)
        scores = torch.arange(20, dtype=torch.float32)
        _simplify_per_trajectory(mask, scores, [(0, 20)], compression_ratio=0.5, min_points_per_trajectory=2)
        expected = max(2, int(0.5 * 20))
        assert mask.sum().item() == expected

    def test_endpoints_always_retained(self):
        """First and last points must be in the retained set."""
        mask = torch.zeros(10, dtype=torch.bool)
        # Give interior points maximum scores — endpoints have lowest scores.
        scores = torch.zeros(10)
        scores[1:-1] = 1.0
        _simplify_per_trajectory(mask, scores, [(0, 10)], compression_ratio=0.3, min_points_per_trajectory=2)
        assert mask[0].item(),  "Start point must be retained"
        assert mask[9].item(),  "End point must be retained"

    def test_min_floor_beats_ratio(self):
        """When ratio * traj_len < min_points, the minimum floor applies."""
        mask = torch.zeros(10, dtype=torch.bool)
        scores = torch.rand(10)
        _simplify_per_trajectory(mask, scores, [(0, 10)], compression_ratio=0.05, min_points_per_trajectory=6)
        assert mask.sum().item() == 6

    def test_multi_trajectory_budgets(self):
        """Each trajectory gets its own independent budget."""
        mask = torch.zeros(30, dtype=torch.bool)
        scores = torch.rand(30)
        boundaries = [(0, 10), (10, 20), (20, 30)]
        _simplify_per_trajectory(mask, scores, boundaries, compression_ratio=0.3, min_points_per_trajectory=2)
        expected_per_traj = max(2, int(0.3 * 10))
        for start, end in boundaries:
            assert mask[start:end].sum().item() == expected_per_traj

    def test_full_short_trajectory_kept(self):
        """A trajectory shorter than points_to_keep keeps all its points."""
        mask = torch.zeros(3, dtype=torch.bool)
        scores = torch.tensor([0.1, 0.5, 0.9])
        _simplify_per_trajectory(mask, scores, [(0, 3)], compression_ratio=0.5, min_points_per_trajectory=5)
        assert mask.all().item()

    def test_scores_not_mutated(self):
        """The original scores tensor must not be modified by the helper."""
        scores = torch.tensor([0.1, 0.9, 0.5, 0.3, 0.7])
        original = scores.clone()
        mask = torch.zeros(5, dtype=torch.bool)
        _simplify_per_trajectory(mask, scores, [(0, 5)], compression_ratio=0.4, min_points_per_trajectory=2)
        assert torch.allclose(scores, original), "scores must not be mutated"


class TestTurnAwareSimplification:
    """Tests for simplify_trajectories with TurnAwareQDSModel and turn_bias_weight."""

    def test_basic_output_shape(self):
        pts, queries = _make_points_and_queries()
        model = TurnAwareQDSModel()
        simplified, mask, scores = simplify_trajectories(
            pts, model, queries, threshold=0.5,
        )
        assert simplified.shape[1] == 8
        assert mask.shape == (pts.shape[0],)
        assert scores.shape == (pts.shape[0],)

    def test_endpoints_retained(self):
        pts, queries = _make_points_and_queries()
        model = TurnAwareQDSModel()
        _, mask, _ = simplify_trajectories(
            pts, model, queries, threshold=0.99,
        )
        assert mask[0].item(),  "First point must be retained"
        assert mask[-1].item(), "Last point must be retained"

    def test_turn_bias_weight_is_additive(self):
        """turn_bias_weight > 0 should not reduce the number of retained points below no-bias."""
        pts, queries = _make_points_and_queries(n=30)
        model = TrajectoryQDSModel()
        # Use a moderate threshold so some points are removed.
        _, mask_no_bias, scores_no_bias = simplify_trajectories(
            pts, model, queries, threshold=0.0,
            compression_ratio=None,
            turn_bias_weight=0.0,
        )
        _, mask_biased, scores_biased = simplify_trajectories(
            pts, model, queries, threshold=0.0,
            compression_ratio=None,
            turn_bias_weight=0.2,
        )
        # With threshold=0 and no bias both should retain all points, but the
        # scores array should differ when bias is applied.
        assert not torch.allclose(scores_no_bias, scores_biased), \
            "Scores must differ when turn_bias_weight > 0 and turn_score column present"

    def test_turn_bias_weight_no_column(self):
        """turn_bias_weight with 7-feature points (no turn_score col) must not crash."""
        pts_7, queries = _make_points_and_queries()
        pts_7 = pts_7[:, :7]  # drop turn_score column
        model = TrajectoryQDSModel()
        simplified, mask, scores = simplify_trajectories(
            pts_7, model, queries, threshold=0.5,
            turn_bias_weight=0.2,
        )
        assert simplified.shape[1] == 7

    def test_per_traj_turn_aware(self):
        """TurnAwareQDSModel works in per-trajectory compression mode."""
        pts, queries = _make_points_and_queries(n=40)
        model = TurnAwareQDSModel()
        _, mask, _ = simplify_trajectories(
            pts, model, queries,
            trajectory_boundaries=[(0, 40)],
            compression_ratio=0.25,
            min_points_per_trajectory=3,
            turn_bias_weight=0.1,
        )
        expected = max(3, int(0.25 * 40))
        assert mask.sum().item() == expected


class TestFallbackBehavior:
    def test_baseline_falls_back_on_degenerate_scores(self):
        """Degenerate model scores should fallback to query-driven scores."""
        n = 120
        pts, queries = _make_points_and_queries(n=n)

        query_scores = torch.linspace(0.0, 1.0, n)
        model_scores = torch.full((n,), 0.33)

        baseline_model = _FixedBaselineModel(model_scores)

        _, _, scores = simplify_trajectories(
            pts,
            baseline_model,
            queries,
            query_scores=query_scores,
            compression_ratio=None,
            threshold=0.5,
            model_max_points=50,
        )

        assert torch.allclose(
            scores, query_scores, atol=1e-6
        ), "Degenerate model scores should fallback to query scores"
