"""Pytest fixtures for AIS-QDS v2 test suite. See src/experiments/README.md for details."""

from __future__ import annotations

import pytest

from src.data.ais_loader import generate_synthetic_ais_data
from src.data.trajectory_dataset import TrajectoryDataset


@pytest.fixture
def synthetic_dataset() -> tuple[list, object]:
    """Create synthetic trajectories and dataset helper. See src/data/README.md for details."""
    traj = generate_synthetic_ais_data(n_ships=12, n_points_per_ship=120, seed=123)
    ds = TrajectoryDataset(traj)
    return traj, ds
