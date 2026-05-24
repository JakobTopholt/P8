"""Feature scaling utilities persisted across training and inference. See src/training/README.md for details."""

from __future__ import annotations

from dataclasses import dataclass

import torch

from src.models.trajectory_qds_model import normalize_points_and_queries


@dataclass
class FeatureScaler:
    """Min-max scaler for points and query features. See src/training/README.md for details."""

    point_min: torch.Tensor
    point_max: torch.Tensor
    query_min: torch.Tensor
    query_max: torch.Tensor

    @classmethod
    def fit(cls, points: torch.Tensor, queries: torch.Tensor) -> "FeatureScaler":
        """Fit scaler statistics from training points and training queries. See src/training/README.md for details."""
        if not torch.isfinite(points).all():
            bad = (~torch.isfinite(points)).any(dim=0).nonzero(as_tuple=False).flatten().tolist()
            raise ValueError(
                f"Non-finite values in training points at feature columns {bad}. "
                "Clean NaN/Inf rows in the data loader before training."
            )
        if not torch.isfinite(queries).all():
            raise ValueError("Non-finite values in query features; check query generator.")
        return cls(
            point_min=points.min(dim=0).values,
            point_max=points.max(dim=0).values,
            query_min=queries.min(dim=0).values,
            query_max=queries.max(dim=0).values,
        )

    def transform(self, points: torch.Tensor, queries: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Transform points and queries with fitted statistics. See src/training/README.md for details."""
        return normalize_points_and_queries(
            points=points,
            queries=queries,
            point_min=self.point_min.to(points.device),
            point_max=self.point_max.to(points.device),
            query_min=self.query_min.to(queries.device),
            query_max=self.query_max.to(queries.device),
        )

    def to_dict(self) -> dict:
        """Serialize scaler statistics. See src/training/README.md for details."""
        return {
            "point_min": self.point_min.tolist(),
            "point_max": self.point_max.tolist(),
            "query_min": self.query_min.tolist(),
            "query_max": self.query_max.tolist(),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "FeatureScaler":
        """Deserialize scaler statistics. See src/training/README.md for details."""
        return cls(
            point_min=torch.tensor(payload["point_min"], dtype=torch.float32),
            point_max=torch.tensor(payload["point_max"], dtype=torch.float32),
            query_min=torch.tensor(payload["query_min"], dtype=torch.float32),
            query_max=torch.tensor(payload["query_max"], dtype=torch.float32),
        )
