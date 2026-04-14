"""Turn-aware QDS model for AIS trajectories. See src/models/README.md for architecture details."""

import torch
import torch.nn as nn
from torch import Tensor

from src.models.attention_utils import chunked_cross_attention_context


class TurnAwareQDSModel(nn.Module):
    """Turn-aware QDS model for AIS trajectory data."""

    #: Number of point features expected by this model.
    POINT_FEATURES: int = 8

    def __init__(self, embed_dim: int = 64, num_heads: int = 4) -> None:
        super().__init__()

        # --- Point encoder: [time, lat, lon, speed, heading, is_start, is_end, turn_score] → embed_dim ---
        self.point_encoder = nn.Sequential(
            nn.Linear(self.POINT_FEATURES, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )

        # --- Query encoder: [lat_min, lat_max, lon_min, lon_max, t_start, t_end] → embed_dim ---
        self.query_encoder = nn.Sequential(
            nn.Linear(6, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )

        # --- Cross-attention (queries attend to points) ---
        self.cross_attention = nn.MultiheadAttention(
            embed_dim=embed_dim,
            num_heads=num_heads,
            batch_first=True,
        )
        self.query_chunk_size = 128

        # --- Importance predictor ---
        self.importance_predictor = nn.Sequential(
            nn.Linear(embed_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid(),
        )

    def forward(self, points: Tensor, queries: Tensor) -> Tensor:
        """Predict per-point importance scores."""
        # Ensure queries has a batch-compatible shape; handle single-query case
        if queries.dim() == 1:
            queries = queries.unsqueeze(0)

        point_emb = self.point_encoder(points).unsqueeze(0)   # [1, N, D]
        query_emb = self.query_encoder(queries).unsqueeze(0)  # [1, M, D]

        # Queries (Q) attend to points (K, V); accumulate contexts in chunks.
        per_point_context = chunked_cross_attention_context(
            self.cross_attention,
            point_emb,
            query_emb,
            self.query_chunk_size,
        )

        point_features = point_emb.squeeze(0) + per_point_context  # [N, D]
        scores = self.importance_predictor(point_features).squeeze(-1)
        return scores
