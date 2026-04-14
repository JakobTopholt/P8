"""Tests for typed query types, dispatcher, and typed generators."""

from __future__ import annotations

import math

import pytest
import torch

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.queries.query_types import (
    QUERY_TYPE_AGGREGATION,
    QUERY_TYPE_INTERSECTION,
    QUERY_TYPE_NEAREST,
    QUERY_TYPE_RANGE,
    execute_query,
    execute_typed_queries,
)
from src.queries.query_generator import (
    generate_aggregation_queries,
    generate_intersection_queries,
    generate_multi_type_workload,
    generate_nearest_neighbor_queries,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_points() -> torch.Tensor:
    """5 points: [time, lat, lon, speed, heading]."""
    return torch.tensor(
        [
            [0.0, 50.0, 0.0, 5.0, 0.0],
            [1.0, 51.0, 1.0, 10.0, 90.0],
            [2.0, 52.0, 2.0, 15.0, 180.0],
            [3.0, 53.0, 3.0, 20.0, 270.0],
            [4.0, 54.0, 4.0, 25.0, 360.0],
        ]
    )


def _make_trajectories(n_ships: int = 3, n_points: int = 20) -> list[torch.Tensor]:
    torch.manual_seed(0)
    trajs = []
    for i in range(n_ships):
        t = torch.linspace(0, 100, n_points).unsqueeze(1)
        lat = 50.0 + torch.rand(n_points, 1) * 5.0
        lon = -5.0 + torch.rand(n_points, 1) * 10.0
        speed = torch.rand(n_points, 1) * 20.0
        heading = torch.rand(n_points, 1) * 360.0
        trajs.append(torch.cat([t, lat, lon, speed, heading], dim=1))
    return trajs


# ---------------------------------------------------------------------------
# execute_query — range
# ---------------------------------------------------------------------------


class TestExecuteQueryRange:
    def _make_range_query(self, **overrides) -> dict:
        defaults = dict(lat_min=49.0, lat_max=55.0, lon_min=-1.0, lon_max=5.0, time_start=-1.0, time_end=5.0)
        defaults.update(overrides)
        return {"type": QUERY_TYPE_RANGE, "params": defaults}

    def test_all_points(self):
        pts = _make_points()
        q = self._make_range_query()
        result = execute_query(q, pts)
        assert abs(result - 75.0) < 1e-4

    def test_no_points(self):
        pts = _make_points()
        q = self._make_range_query(lat_min=60.0, lat_max=65.0)
        result = execute_query(q, pts)
        assert result == 0.0


# ---------------------------------------------------------------------------
# execute_query — aggregation
# ---------------------------------------------------------------------------


class TestExecuteQueryAggregation:
    def _make_agg_query(self, **overrides) -> dict:
        defaults = dict(lat_min=49.0, lat_max=55.0, lon_min=-1.0, lon_max=5.0, time_start=-1.0, time_end=5.0)
        defaults.update(overrides)
        return {"type": QUERY_TYPE_AGGREGATION, "params": defaults}

    def test_all_points_in_region(self):
        pts = _make_points()
        q = self._make_agg_query()
        result = execute_query(q, pts)
        assert result == 5.0

    def test_no_points_in_region(self):
        pts = _make_points()
        q = self._make_agg_query(lat_min=60.0, lat_max=65.0)
        result = execute_query(q, pts)
        assert result == 0.0

    def test_partial_points(self):
        pts = _make_points()
        q = self._make_agg_query(lat_min=50.5, lat_max=51.5)
        result = execute_query(q, pts)
        assert result == 1.0

    def test_empty_tensor(self):
        pts = torch.zeros(0, 5)
        q = self._make_agg_query()
        result = execute_query(q, pts)
        assert result == 0.0

    def test_returns_float(self):
        pts = _make_points()
        q = self._make_agg_query()
        result = execute_query(q, pts)
        assert isinstance(result, float)


# ---------------------------------------------------------------------------
# execute_query — intersection
# ---------------------------------------------------------------------------


class TestExecuteQueryIntersection:
    def _make_int_query(self, **overrides) -> dict:
        defaults = dict(lat_min=49.0, lat_max=55.0, lon_min=-1.0, lon_max=5.0, time_start=-1.0, time_end=5.0)
        defaults.update(overrides)
        return {"type": QUERY_TYPE_INTERSECTION, "params": defaults}

    def test_no_trajectories_falls_back_to_point_count(self):
        pts = _make_points()
        q = self._make_int_query()
        result = execute_query(q, pts, trajectories=None)
        assert result == 5.0  # fallback: count matching points

    def test_with_trajectories_counts_ships(self):
        trajs = _make_trajectories(n_ships=3)
        pts = torch.cat(trajs, dim=0)
        q = self._make_int_query(lat_min=49.0, lat_max=60.0, lon_min=-10.0, lon_max=10.0)
        result = execute_query(q, pts, trajectories=trajs)
        assert 0 <= result <= 3.0

    def test_no_intersection(self):
        trajs = _make_trajectories(n_ships=3)
        pts = torch.cat(trajs, dim=0)
        q = self._make_int_query(lat_min=80.0, lat_max=90.0, lon_min=100.0, lon_max=110.0)
        result = execute_query(q, pts, trajectories=trajs)
        assert result == 0.0

    def test_all_trajectories_intersect(self):
        # Trajectories span lat 50-55, lon -5 to 5
        trajs = _make_trajectories(n_ships=3)
        pts = torch.cat(trajs, dim=0)
        q = self._make_int_query(lat_min=49.0, lat_max=60.0, lon_min=-10.0, lon_max=10.0, time_start=-1.0, time_end=200.0)
        result = execute_query(q, pts, trajectories=trajs)
        assert result == 3.0


# ---------------------------------------------------------------------------
# execute_query — nearest
# ---------------------------------------------------------------------------


class TestExecuteQueryNearest:
    def _make_nn_query(self, **overrides) -> dict:
        defaults = dict(query_lat=51.0, query_lon=1.0, query_time=1.0, time_window=0.5)
        defaults.update(overrides)
        return {"type": QUERY_TYPE_NEAREST, "params": defaults}

    def test_returns_non_negative(self):
        pts = _make_points()
        q = self._make_nn_query()
        result = execute_query(q, pts)
        assert result >= 0.0

    def test_exact_match_gives_zero(self):
        pts = _make_points()
        # Point 1 is exactly at (51, 1) at time 1
        q = self._make_nn_query(query_lat=51.0, query_lon=1.0, query_time=1.0)
        result = execute_query(q, pts)
        assert result < 1e-5

    def test_no_points_in_window_falls_back(self):
        pts = _make_points()
        # Time window far outside any data — should still return a finite result
        q = self._make_nn_query(query_time=1000.0, time_window=0.1)
        result = execute_query(q, pts)
        assert math.isfinite(result)

    def test_empty_tensor(self):
        pts = torch.zeros(0, 5)
        q = self._make_nn_query()
        result = execute_query(q, pts)
        assert math.isinf(result)

    def test_returns_float(self):
        pts = _make_points()
        q = self._make_nn_query()
        result = execute_query(q, pts)
        assert isinstance(result, float)

    # --- kNN tests ---

    def test_k1_equals_min_distance(self):
        """k=1 must return the same value as the original single-NN."""
        pts = _make_points()
        q_k1 = self._make_nn_query(k=1)
        q_default = self._make_nn_query()  # no k → defaults to 1
        assert execute_query(q_k1, pts) == execute_query(q_default, pts)

    def test_k_greater_than_one_returns_mean(self):
        """k>1 should return the mean of k smallest distances (>= min dist)."""
        pts = _make_points()
        q_k1 = self._make_nn_query(query_time=1.0, k=1)
        q_k3 = self._make_nn_query(query_time=1.0, k=3)
        dist_k1 = execute_query(q_k1, pts)
        dist_k3 = execute_query(q_k3, pts)
        # Mean of 3 distances must be >= the single minimum distance
        assert dist_k3 >= dist_k1 - 1e-9

    def test_k_larger_than_pool_uses_all(self):
        """When k > available points the result is still finite."""
        pts = _make_points()  # 5 points
        q = self._make_nn_query(k=100, time_window=100.0)
        result = execute_query(q, pts)
        assert math.isfinite(result)
        assert result >= 0.0

    def test_knn_monotone_in_k(self):
        """Mean distance for k+1 nearest neighbours must be >= mean for k."""
        pts = _make_points()
        prev = execute_query(self._make_nn_query(k=1, time_window=10.0), pts)
        for k in range(2, 6):
            curr = execute_query(self._make_nn_query(k=k, time_window=10.0), pts)
            assert curr >= prev - 1e-9, f"k={k} mean dist {curr} < k={k-1} mean dist {prev}"
            prev = curr


# ---------------------------------------------------------------------------
# execute_query — unknown type
# ---------------------------------------------------------------------------


class TestExecuteQueryUnknownType:
    def test_raises_value_error(self):
        pts = _make_points()
        q = {"type": "bogus", "params": {}}
        with pytest.raises(ValueError, match="Unknown query type"):
            execute_query(q, pts)


# ---------------------------------------------------------------------------
# execute_typed_queries
# ---------------------------------------------------------------------------


class TestExecuteTypedQueries:
    def test_returns_list_of_floats(self):
        pts = _make_points()
        queries = [
            {"type": QUERY_TYPE_AGGREGATION, "params": dict(lat_min=49.0, lat_max=55.0, lon_min=-1.0, lon_max=5.0, time_start=-1.0, time_end=5.0)},
            {"type": QUERY_TYPE_NEAREST, "params": dict(query_lat=51.0, query_lon=1.0, query_time=1.0, time_window=0.5)},
        ]
        results = execute_typed_queries(queries, pts)
        assert len(results) == 2
        assert all(isinstance(r, float) for r in results)

    def test_empty_list(self):
        pts = _make_points()
        results = execute_typed_queries([], pts)
        assert results == []


# ---------------------------------------------------------------------------
# generate_intersection_queries
# ---------------------------------------------------------------------------


class TestGenerateIntersectionQueries:
    def test_count(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=20)
        assert len(qs) == 20

    def test_type_field(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=10)
        assert all(q["type"] == QUERY_TYPE_INTERSECTION for q in qs)

    def test_params_keys(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=5)
        required = {"lat_min", "lat_max", "lon_min", "lon_max", "time_start", "time_end"}
        for q in qs:
            assert required <= set(q["params"]), f"Missing keys in {q['params']}"

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        qs = generate_intersection_queries(trajs, n_queries=50)
        for q in qs:
            p = q["params"]
            assert p["lat_min"] < p["lat_max"]
            assert p["lon_min"] < p["lon_max"]
            assert p["time_start"] < p["time_end"]


# ---------------------------------------------------------------------------
# generate_aggregation_queries
# ---------------------------------------------------------------------------


class TestGenerateAggregationQueries:
    def test_count(self):
        trajs = _make_trajectories()
        qs = generate_aggregation_queries(trajs, n_queries=30)
        assert len(qs) == 30

    def test_type_field(self):
        trajs = _make_trajectories()
        qs = generate_aggregation_queries(trajs, n_queries=10)
        assert all(q["type"] == QUERY_TYPE_AGGREGATION for q in qs)

    def test_params_keys(self):
        trajs = _make_trajectories()
        qs = generate_aggregation_queries(trajs, n_queries=5)
        required = {"lat_min", "lat_max", "lon_min", "lon_max", "time_start", "time_end"}
        for q in qs:
            assert required <= set(q["params"])

    def test_min_less_than_max(self):
        trajs = _make_trajectories()
        qs = generate_aggregation_queries(trajs, n_queries=50)
        for q in qs:
            p = q["params"]
            assert p["lat_min"] < p["lat_max"]
            assert p["lon_min"] < p["lon_max"]
            assert p["time_start"] < p["time_end"]


# ---------------------------------------------------------------------------
# generate_nearest_neighbor_queries
# ---------------------------------------------------------------------------


class TestGenerateNearestNeighborQueries:
    def test_count(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=25)
        assert len(qs) == 25

    def test_type_field(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=10)
        assert all(q["type"] == QUERY_TYPE_NEAREST for q in qs)

    def test_params_keys(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5)
        required = {"query_lat", "query_lon", "query_time", "time_window", "k"}
        for q in qs:
            assert required <= set(q["params"])

    def test_time_window_positive(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=10)
        for q in qs:
            assert q["params"]["time_window"] > 0.0

    def test_default_k_is_1(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5)
        assert all(q["params"]["k"] == 1 for q in qs)

    def test_custom_k_stored_in_params(self):
        trajs = _make_trajectories()
        qs = generate_nearest_neighbor_queries(trajs, n_queries=5, k=7)
        assert all(q["params"]["k"] == 7 for q in qs)

    def test_invalid_k_raises(self):
        trajs = _make_trajectories()
        with pytest.raises(ValueError, match="k must be >= 1"):
            generate_nearest_neighbor_queries(trajs, n_queries=5, k=0)


# ---------------------------------------------------------------------------
# generate_multi_type_workload
# ---------------------------------------------------------------------------


class TestGenerateMultiTypeWorkload:
    def test_default_count(self):
        trajs = _make_trajectories()
        qs = generate_multi_type_workload(trajs, total_queries=40)
        assert len(qs) == 40

    def test_custom_ratios(self):
        trajs = _make_trajectories()
        qs = generate_multi_type_workload(
            trajs,
            total_queries=100,
            ratios={"range": 0.4, "intersection": 0.2, "aggregation": 0.2, "nearest": 0.2},
        )
        assert len(qs) == 100

    def test_type_distribution(self):
        trajs = _make_trajectories()
        qs = generate_multi_type_workload(
            trajs,
            total_queries=100,
            ratios={"range": 0.5, "nearest": 0.5},
        )
        types = [q["type"] for q in qs]
        assert set(types) == {"range", "nearest"}
        assert types.count("range") + types.count("nearest") == 100

    def test_invalid_ratio_sum(self):
        trajs = _make_trajectories()
        with pytest.raises(ValueError, match="sum to 1.0"):
            generate_multi_type_workload(
                trajs,
                total_queries=10,
                ratios={"range": 0.3, "nearest": 0.3},
            )

    def test_unknown_query_type_in_ratios(self):
        trajs = _make_trajectories()
        with pytest.raises(ValueError, match="Unknown query type"):
            generate_multi_type_workload(
                trajs,
                total_queries=10,
                ratios={"range": 0.5, "bogus": 0.5},
            )

    def test_all_dicts_have_type_and_params(self):
        trajs = _make_trajectories()
        qs = generate_multi_type_workload(trajs, total_queries=20)
        for q in qs:
            assert "type" in q
            assert "params" in q
