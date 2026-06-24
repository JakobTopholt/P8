"""Tests no cross-trajectory leakage in batched forward passes. See src/models/README.md for details."""

from __future__ import annotations

import torch

from src.models.trajectory_qds_model import TrajectoryQDSModel


def test_no_cross_trajectory_attention_leakage() -> None:
    """Assert zero trajectory predictions are unaffected by other trajectories. See src/models/README.md for details."""
    model = TrajectoryQDSModel(point_dim=7, query_dim=12)
    model.eval()

    traj_a = torch.zeros((1, 64, 7), dtype=torch.float32)
    traj_b1 = torch.zeros((1, 64, 7), dtype=torch.float32)
    traj_b2 = torch.randn((1, 64, 7), dtype=torch.float32)
    queries = torch.randn((20, 12), dtype=torch.float32)
    q_ids = torch.zeros((20,), dtype=torch.long)
    pad = torch.zeros((2, 64), dtype=torch.bool)

    x1 = torch.cat([traj_a, traj_b1], dim=0)
    x2 = torch.cat([traj_a, traj_b2], dim=0)

    with torch.no_grad():
        y1 = model(points=x1, queries=queries, query_type_ids=q_ids, padding_mask=pad)[0]
        y2 = model(points=x2, queries=queries, query_type_ids=q_ids, padding_mask=pad)[0]

    assert torch.allclose(y1, y2, atol=1e-6)
