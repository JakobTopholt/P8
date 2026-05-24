"""Tests required query_type_ids path for model training mode. See src/models/README.md for details."""

from __future__ import annotations

import pytest
import torch

from src.models.trajectory_qds_model import TrajectoryQDSModel


def test_query_type_ids_required() -> None:
    """Assert training forward fails without query_type_ids. See src/models/README.md for details."""
    model = TrajectoryQDSModel(point_dim=7, query_dim=12)
    model.train()
    points = torch.randn(1, 32, 7)
    queries = torch.randn(8, 12)
    with pytest.raises(RuntimeError):
        _ = model(points=points, queries=queries, query_type_ids=None)
