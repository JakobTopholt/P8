"""Tests typed query execution semantics. See src/queries/README.md for details."""

from __future__ import annotations

import torch

from src.queries.query_executor import execute_knn_query, execute_similarity_query


def test_knn_returns_nearest_distinct_trajectories() -> None:
    """Assert kNN selects nearest trajectories instead of duplicate points from one vessel."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.000, 1.0],
            [0.0, 0.0, 0.001, 1.0],
            [0.0, 0.0, 0.002, 1.0],
            [0.0, 0.0, 0.010, 1.0],
            [0.0, 1.0, 1.000, 1.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 3), (3, 4), (4, 5)]
    params = {
        "lat": 0.0,
        "lon": 0.0,
        "t_center": 0.0,
        "t_half_window": 1.0,
        "k": 2,
    }

    assert execute_knn_query(points, params, boundaries) == {0, 1}


def test_similarity_distance_uses_lat_lon_not_time_lat() -> None:
    """Assert similarity ranks spatial geometry independently of timestamp magnitude."""
    reference = [[0.0, 0.0, 0.0], [1.0, 0.0, 0.1]]
    trajectories = [
        torch.tensor([[1000.0, 0.0, 0.0], [1001.0, 0.0, 0.1]], dtype=torch.float32),
        torch.tensor([[0.0, 1.0, 1.0], [1.0, 1.0, 1.1]], dtype=torch.float32),
    ]
    params = {
        "lat_query_centroid": 0.5,
        "lon_query_centroid": 0.55,
        "t_start": 0.0,
        "t_end": 2000.0,
        "radius": 10.0,
        "top_k": 1,
    }

    assert execute_similarity_query(trajectories, params, reference) == [0]
