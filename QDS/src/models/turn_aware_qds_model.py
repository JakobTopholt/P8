"""Turn-aware model wrapper using the same typed architecture. See src/models/README.md for details."""

from __future__ import annotations

from src.models.trajectory_qds_model import TrajectoryQDSModel


class TurnAwareQDSModel(TrajectoryQDSModel):
    """Turn-aware variant that consumes the extra turn-score feature. See src/models/README.md for details."""

    pass
