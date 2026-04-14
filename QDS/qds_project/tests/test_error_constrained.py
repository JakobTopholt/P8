"""Tests for error-constrained simplification (binary search over importance threshold)."""
import pytest
import torch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.evaluation.metrics import compute_query_error, compute_compression_metrics
from src.simplification.simplify_trajectories import apply_threshold_simplification
from src.experiments.experiment_pipeline_helpers import find_optimal_threshold


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_points_and_queries(n=60):
    torch.manual_seed(42)
    pts = torch.zeros(n, 7)
    pts[:, 0] = torch.arange(n).float()
    pts[:, 1] = 50.0 + torch.rand(n)
    pts[:, 2] = torch.rand(n)
    pts[:, 3] = torch.rand(n) * 20.0
    pts[:, 4] = torch.rand(n) * 360.0
    pts[0, 5] = 1.0   # is_start
    queries = torch.tensor([
        [50.0, 51.0, 0.0, 1.0, 0.0, 30.0],
        [50.2, 50.8, 0.2, 0.8, 10.0, 50.0],
    ])
    return pts, queries


def _make_boundaries(n=60):
    return [(0, n)]


# ---------------------------------------------------------------------------
# apply_threshold_simplification
# ---------------------------------------------------------------------------

class TestApplyThresholdSimplification:
    def test_returns_two_values(self):
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        result = apply_threshold_simplification(pts, scores, threshold=0.5, boundaries=boundaries)
        assert len(result) == 2

    def test_simplified_points_are_subset(self):
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        simplified, mask = apply_threshold_simplification(pts, scores, 0.5, boundaries)
        assert simplified.shape[0] == mask.sum().item()
        assert simplified.shape[1] == pts.shape[1]

    def test_mask_is_boolean(self):
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        _, mask = apply_threshold_simplification(pts, scores, 0.5, boundaries)
        assert mask.dtype == torch.bool
        assert mask.shape == (pts.shape[0],)

    def test_threshold_zero_keeps_all(self):
        """threshold=0 should keep all points."""
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        simplified, mask = apply_threshold_simplification(pts, scores, 0.0, boundaries)
        assert mask.all().item()
        assert simplified.shape[0] == pts.shape[0]

    def test_threshold_above_max_falls_back(self):
        """threshold > max score should still keep at least one point."""
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])  # all in [0, 1)
        boundaries = _make_boundaries()
        _, mask = apply_threshold_simplification(pts, scores, threshold=2.0, boundaries=boundaries)
        assert mask.sum().item() >= 1

    def test_endpoints_always_retained(self):
        """First and last points of each trajectory must always be in the retained set."""
        pts, _ = _make_points_and_queries(n=30)
        scores = torch.rand(30)
        boundaries = [(0, 30)]
        _, mask = apply_threshold_simplification(pts, scores, threshold=0.99, boundaries=boundaries)
        assert mask[0].item(), "First point must be retained"
        assert mask[29].item(), "Last point must be retained"

    def test_min_points_per_trajectory_enforced(self):
        pts, _ = _make_points_and_queries(n=20)
        # Give all points a score below threshold so only endpoints survive.
        scores = torch.zeros(20)
        boundaries = [(0, 20)]
        _, mask = apply_threshold_simplification(
            pts, scores, threshold=0.5, boundaries=boundaries, min_points_per_trajectory=5
        )
        assert mask.sum().item() >= 5

    def test_multi_trajectory_endpoints_retained(self):
        torch.manual_seed(7)
        pts = torch.zeros(40, 7)
        pts[:, 0] = torch.arange(40).float()
        pts[:, 1] = 50.0 + torch.rand(40)
        scores = torch.rand(40)
        boundaries = [(0, 20), (20, 40)]
        _, mask = apply_threshold_simplification(pts, scores, threshold=0.99, boundaries=boundaries)
        assert mask[0].item(),  "Start of traj 1"
        assert mask[19].item(), "End of traj 1"
        assert mask[20].item(), "Start of traj 2"
        assert mask[39].item(), "End of traj 2"

    def test_deterministic(self):
        """Same inputs should produce identical outputs on repeated calls."""
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        _, mask1 = apply_threshold_simplification(pts, scores, 0.5, boundaries)
        _, mask2 = apply_threshold_simplification(pts, scores, 0.5, boundaries)
        assert torch.equal(mask1, mask2)


# ---------------------------------------------------------------------------
# compute_query_error
# ---------------------------------------------------------------------------

class TestComputeQueryError:
    def test_returns_two_floats(self):
        pts, queries = _make_points_and_queries()
        mean_err, max_err = compute_query_error(pts, pts, queries)
        assert isinstance(mean_err, float)
        assert isinstance(max_err, float)

    def test_identical_datasets_zero_error(self):
        pts, queries = _make_points_and_queries()
        mean_err, max_err = compute_query_error(pts, pts, queries)
        assert mean_err == pytest.approx(0.0, abs=1e-6)
        assert max_err == pytest.approx(0.0, abs=1e-6)

    def test_non_negative(self):
        pts, queries = _make_points_and_queries()
        simplified = pts[:30]
        mean_err, max_err = compute_query_error(pts, simplified, queries)
        assert mean_err >= 0.0
        assert max_err >= 0.0

    def test_max_error_ge_mean_error(self):
        pts, queries = _make_points_and_queries()
        simplified = pts[:30]
        mean_err, max_err = compute_query_error(pts, simplified, queries)
        assert max_err >= mean_err - 1e-9

    def test_precomputed_original_results(self):
        """Passing precomputed original_results should give identical output."""
        from src.queries.query_executor import run_queries
        pts, queries = _make_points_and_queries()
        simplified = pts[:30]
        orig_res = run_queries(pts, queries)
        mean1, max1 = compute_query_error(pts, simplified, queries)
        mean2, max2 = compute_query_error(pts, simplified, queries, original_results=orig_res)
        assert mean1 == pytest.approx(mean2, abs=1e-9)
        assert max1 == pytest.approx(max2, abs=1e-9)

    def test_empty_simplified(self):
        pts, queries = _make_points_and_queries()
        empty = torch.zeros(0, pts.shape[1])
        mean_err, max_err = compute_query_error(pts, empty, queries)
        assert mean_err >= 0.0


# ---------------------------------------------------------------------------
# compute_compression_metrics
# ---------------------------------------------------------------------------

class TestComputeCompressionMetrics:
    def test_keys_present(self):
        pts, _ = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        simplified, mask = apply_threshold_simplification(pts, scores, 0.5, boundaries)
        metrics = compute_compression_metrics(pts, simplified, boundaries, mask)
        for key in ("compression_ratio", "avg_points_before", "avg_points_after",
                    "trajectories_preserved", "n_trajectories"):
            assert key in metrics

    def test_no_compression(self):
        pts, _ = _make_points_and_queries()
        mask = torch.ones(pts.shape[0], dtype=torch.bool)
        boundaries = _make_boundaries()
        metrics = compute_compression_metrics(pts, pts, boundaries, mask)
        assert metrics["compression_ratio"] == pytest.approx(1.0)
        assert metrics["trajectories_preserved"] == 1
        assert metrics["n_trajectories"] == 1

    def test_half_compression(self):
        pts, _ = _make_points_and_queries(n=60)
        simplified = pts[:30]
        mask = torch.zeros(60, dtype=torch.bool)
        mask[:30] = True
        boundaries = [(0, 60)]
        metrics = compute_compression_metrics(pts, simplified, boundaries, mask)
        assert metrics["compression_ratio"] == pytest.approx(0.5, abs=1e-6)

    def test_multi_trajectory_preserved_count(self):
        pts, _ = _make_points_and_queries(n=60)
        boundaries = [(0, 30), (30, 60)]
        # Retain only points from first trajectory.
        mask = torch.zeros(60, dtype=torch.bool)
        mask[:30] = True
        simplified = pts[:30]
        metrics = compute_compression_metrics(pts, simplified, boundaries, mask)
        assert metrics["trajectories_preserved"] == 1
        assert metrics["n_trajectories"] == 2


# ---------------------------------------------------------------------------
# find_optimal_threshold
# ---------------------------------------------------------------------------

class TestFindOptimalThreshold:
    def test_returns_three_values(self):
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        result = find_optimal_threshold(
            scores, pts, queries, max_query_error=0.5, boundaries=boundaries
        )
        assert len(result) == 3

    def test_achieved_error_within_budget(self):
        """The returned threshold must yield error <= max_query_error + tolerance."""
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        max_err = 0.5
        tol = 1e-3
        best_threshold, mean_err, _ = find_optimal_threshold(
            scores, pts, queries,
            max_query_error=max_err,
            boundaries=boundaries,
            error_tolerance=tol,
            max_search_iterations=15,
        )
        # Verify by applying the returned threshold
        simplified, _ = apply_threshold_simplification(pts, scores, best_threshold, boundaries)
        actual_mean, _ = compute_query_error(pts, simplified, queries)
        assert actual_mean <= max_err + tol + 1e-6

    def test_very_tight_budget_keeps_most_points(self):
        """A very small max_query_error budget should result in a low threshold (more points kept)."""
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        best_threshold_tight, _, _ = find_optimal_threshold(
            scores, pts, queries,
            max_query_error=1e-9,
            boundaries=boundaries,
            max_search_iterations=10,
        )
        best_threshold_loose, _, _ = find_optimal_threshold(
            scores, pts, queries,
            max_query_error=0.9,
            boundaries=boundaries,
            max_search_iterations=10,
        )
        # Tighter budget → lower (or equal) threshold → more points retained.
        assert best_threshold_tight <= best_threshold_loose + 1e-6

    def test_max_error_non_negative(self):
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        _, mean_err, max_err = find_optimal_threshold(
            scores, pts, queries, max_query_error=0.3, boundaries=boundaries
        )
        assert mean_err >= 0.0
        assert max_err >= 0.0

    def test_max_error_ge_mean_error(self):
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        _, mean_err, max_err = find_optimal_threshold(
            scores, pts, queries, max_query_error=0.3, boundaries=boundaries
        )
        assert max_err >= mean_err - 1e-9

    def test_single_iteration(self):
        """Binary search with 1 iteration must still return valid results."""
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        best_threshold, mean_err, max_err = find_optimal_threshold(
            scores, pts, queries,
            max_query_error=0.5,
            boundaries=boundaries,
            max_search_iterations=1,
        )
        assert isinstance(best_threshold, float)
        assert mean_err >= 0.0

    def test_deterministic_with_same_inputs(self):
        """find_optimal_threshold should produce deterministic results across calls."""
        pts, queries = _make_points_and_queries()
        scores = torch.rand(pts.shape[0])
        boundaries = _make_boundaries()
        t1, e1, _ = find_optimal_threshold(
            scores, pts, queries, max_query_error=0.4, boundaries=boundaries,
            max_search_iterations=10,
        )
        t2, e2, _ = find_optimal_threshold(
            scores, pts, queries, max_query_error=0.4, boundaries=boundaries,
            max_search_iterations=10,
        )
        assert t1 == pytest.approx(t2, abs=1e-9)
        assert e1 == pytest.approx(e2, abs=1e-9)
