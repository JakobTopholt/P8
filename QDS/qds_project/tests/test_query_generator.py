"""Tests for the AIS query generator (uniform, density-biased, and mixed)."""

import torch
import pytest

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.queries.query_generator import (
    generate_uniform_queries,
    generate_density_biased_queries,
    generate_mixed_queries,
    generate_spatiotemporal_queries,
    generate_intersection_queries,
    generate_aggregation_queries,
    generate_nearest_neighbor_queries,
    generate_multi_type_workload,
)


def _make_trajectories(n_ships: int = 3, n_points: int = 20) -> list[torch.Tensor]:
    """Create a small set of synthetic trajectory tensors for testing.

    Each trajectory tensor has shape [T, 5] with columns:
    [time, lat, lon, speed, heading].
    """
    torch.manual_seed(42)
    trajectories = []
    for i in range(n_ships):
        t = torch.linspace(0, 100, n_points).unsqueeze(1)  # [T, 1]
        lat = 50.0 + torch.rand(n_points, 1) * 5.0
        lon = -5.0 + torch.rand(n_points, 1) * 10.0
        speed = torch.rand(n_points, 1) * 20.0
        heading = torch.rand(n_points, 1) * 360.0
        trajectories.append(torch.cat([t, lat, lon, speed, heading], dim=1))
    return trajectories


class TestGenerateUniformQueries:
    def test_output_shape(self):
        trajs = _make_trajectories()
        queries = generate_uniform_queries(trajs, n_queries=50)
        assert queries.shape == (50, 6), f"Expected (50, 6), got {queries.shape}"

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        queries = generate_uniform_queries(trajs, n_queries=100)
        assert (queries[:, 0] < queries[:, 1]).all(), "lat_min must be < lat_max"
        assert (queries[:, 2] < queries[:, 3]).all(), "lon_min must be < lon_max"
        assert (queries[:, 4] < queries[:, 5]).all(), "time_start must be < time_end"

    def test_within_data_bounds(self):
        trajs = _make_trajectories()
        all_points = torch.cat(trajs, dim=0)
        lat_min, lat_max = float(all_points[:, 1].min()), float(all_points[:, 1].max())
        lon_min, lon_max = float(all_points[:, 2].min()), float(all_points[:, 2].max())
        time_min, time_max = float(all_points[:, 0].min()), float(all_points[:, 0].max())

        queries = generate_uniform_queries(trajs, n_queries=100)

        assert (queries[:, 0] >= lat_min - 1e-4).all()
        assert (queries[:, 1] <= lat_max + 1e-4).all()
        assert (queries[:, 2] >= lon_min - 1e-4).all()
        assert (queries[:, 3] <= lon_max + 1e-4).all()
        assert (queries[:, 4] >= time_min - 1e-4).all()
        assert (queries[:, 5] <= time_max + 1e-4).all()

    def test_single_query(self):
        trajs = _make_trajectories()
        queries = generate_uniform_queries(trajs, n_queries=1)
        assert queries.shape == (1, 6)

    def test_default_n_queries(self):
        trajs = _make_trajectories()
        queries = generate_uniform_queries(trajs)
        assert queries.shape[0] == 100


class TestGenerateDensityBiasedQueries:
    def test_output_shape(self):
        trajs = _make_trajectories()
        queries = generate_density_biased_queries(trajs, n_queries=50)
        assert queries.shape == (50, 6)

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        queries = generate_density_biased_queries(trajs, n_queries=100)
        assert (queries[:, 0] < queries[:, 1]).all(), "lat_min must be < lat_max"
        assert (queries[:, 2] < queries[:, 3]).all(), "lon_min must be < lon_max"
        assert (queries[:, 4] < queries[:, 5]).all(), "time_start must be < time_end"

    def test_within_data_bounds(self):
        trajs = _make_trajectories()
        all_points = torch.cat(trajs, dim=0)
        lat_min, lat_max = float(all_points[:, 1].min()), float(all_points[:, 1].max())
        lon_min, lon_max = float(all_points[:, 2].min()), float(all_points[:, 2].max())
        time_min, time_max = float(all_points[:, 0].min()), float(all_points[:, 0].max())

        queries = generate_density_biased_queries(trajs, n_queries=100)

        assert (queries[:, 0] >= lat_min - 1e-4).all()
        assert (queries[:, 1] <= lat_max + 1e-4).all()
        assert (queries[:, 2] >= lon_min - 1e-4).all()
        assert (queries[:, 3] <= lon_max + 1e-4).all()
        assert (queries[:, 4] >= time_min - 1e-4).all()
        assert (queries[:, 5] <= time_max + 1e-4).all()

    def test_default_n_queries(self):
        trajs = _make_trajectories()
        queries = generate_density_biased_queries(trajs)
        assert queries.shape[0] == 100


class TestGenerateMixedQueries:
    def test_output_shape(self):
        trajs = _make_trajectories()
        queries = generate_mixed_queries(trajs, total_queries=80)
        assert queries.shape == (80, 6)

    def test_density_ratio_one(self):
        """density_ratio=1.0 → all queries from density-biased generator."""
        trajs = _make_trajectories()
        queries = generate_mixed_queries(trajs, total_queries=50, density_ratio=1.0)
        assert queries.shape == (50, 6)

    def test_density_ratio_zero(self):
        """density_ratio=0.0 → all queries from uniform generator."""
        trajs = _make_trajectories()
        queries = generate_mixed_queries(trajs, total_queries=50, density_ratio=0.0)
        assert queries.shape == (50, 6)

    def test_mixed_split(self):
        """density_ratio=0.7 with 10 queries → 7 density + 3 uniform = 10 total."""
        trajs = _make_trajectories()
        queries = generate_mixed_queries(trajs, total_queries=10, density_ratio=0.7)
        assert queries.shape == (10, 6)

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        queries = generate_mixed_queries(trajs, total_queries=100, density_ratio=0.5)
        assert (queries[:, 0] < queries[:, 1]).all()
        assert (queries[:, 2] < queries[:, 3]).all()
        assert (queries[:, 4] < queries[:, 5]).all()

    def test_invalid_density_ratio(self):
        trajs = _make_trajectories()
        with pytest.raises(ValueError):
            generate_mixed_queries(trajs, total_queries=10, density_ratio=1.5)

    def test_default_args(self):
        trajs = _make_trajectories()
        queries = generate_mixed_queries(trajs)
        assert queries.shape == (100, 6)


class TestGenerateSpatiotemporalQueriesBackwardCompat:
    """Verify the legacy function still works and delegates correctly."""

    def test_anchor_to_data_true(self):
        trajs = _make_trajectories()
        queries = generate_spatiotemporal_queries(trajs, n_queries=20, anchor_to_data=True)
        assert queries.shape == (20, 6)

    def test_anchor_to_data_false(self):
        trajs = _make_trajectories()
        queries = generate_spatiotemporal_queries(trajs, n_queries=20, anchor_to_data=False)
        assert queries.shape == (20, 6)

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        queries = generate_spatiotemporal_queries(trajs, n_queries=50)
        assert (queries[:, 0] < queries[:, 1]).all()
        assert (queries[:, 2] < queries[:, 3]).all()
        assert (queries[:, 4] < queries[:, 5]).all()


class TestGenerateIntersectionQueriesInGenerator:
    """Integration tests for generate_intersection_queries in the generator module."""

    def test_output_length(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=20)
        assert len(qs) == 20

    def test_type_field(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=10)
        assert all(q["type"] == "intersection" for q in qs)

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=30)
        for q in qs:
            p = q["params"]
            assert p["lat_min"] < p["lat_max"]
            assert p["lon_min"] < p["lon_max"]
            assert p["time_start"] < p["time_end"]


class TestGenerateAggregationQueriesInGenerator:
    def test_output_length(self):
        trajs = _make_trajectories()
        qs = generate_aggregation_queries(trajs, n_queries=15)
        assert len(qs) == 15

    def test_type_field(self):
        trajs = _make_trajectories()
        qs = generate_aggregation_queries(trajs, n_queries=10)
        assert all(q["type"] == "aggregation" for q in qs)


class TestGenerateNearestNeighborQueriesInGenerator:
    def test_output_length(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=12)
        assert len(qs) == 12

    def test_type_field(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5)
        assert all(q["type"] == "nearest" for q in qs)

    def test_params_present(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5)
        for q in qs:
            assert {"query_lat", "query_lon", "query_time", "time_window", "k"} <= set(q["params"])

    def test_default_k_is_1(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5)
        assert all(q["params"]["k"] == 1 for q in qs)

    def test_custom_k(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5, k=5)
        assert all(q["params"]["k"] == 5 for q in qs)


class TestGenerateMultiTypeWorkloadInGenerator:
    def test_total_count(self):
        trajs = _make_trajectories()
        qs = generate_multi_type_workload(trajs, total_queries=80)
        assert len(qs) == 80

    def test_contains_all_four_types(self):
        trajs = _make_trajectories()
        qs = generate_multi_type_workload(trajs, total_queries=100)
        types = {q["type"] for q in qs}
        assert types == {"range", "intersection", "aggregation", "nearest"}

    def test_custom_ratios_sum_to_one(self):
        trajs = _make_trajectories()
        ratios = {"range": 0.5, "aggregation": 0.5}
        qs = generate_multi_type_workload(trajs, total_queries=20, ratios=ratios)
        assert len(qs) == 20
        types = {q["type"] for q in qs}
        assert types <= {"range", "aggregation"}

    def test_invalid_sum_raises(self):
        trajs = _make_trajectories()
        with pytest.raises(ValueError):
            generate_multi_type_workload(trajs, total_queries=10, ratios={"range": 0.3, "nearest": 0.3})

    def test_unknown_type_raises(self):
        trajs = _make_trajectories()
        with pytest.raises(ValueError):
            generate_multi_type_workload(trajs, total_queries=10, ratios={"range": 0.5, "bogus": 0.5})
