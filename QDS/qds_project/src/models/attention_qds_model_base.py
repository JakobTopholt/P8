"""Shared attention-based architecture for QDS model variants."""

from __future__ import annotations

import torch
import torch.nn as nn
from torch import Tensor

from src.models.attention_utils import chunked_cross_attention_context


class AttentionQDSModelBase(nn.Module):
    """Common point/query attention backbone used by QDS models."""

    def __init__(
        self,
        *,
        point_features: int,
        embed_dim: int = 64,
        num_heads: int = 4,
    ) -> None:
        super().__init__()

        self.point_encoder = nn.Sequential(
            nn.Linear(point_features, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )

        self.query_encoder = nn.Sequential(
            nn.Linear(6, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )

        self.cross_attention = nn.MultiheadAttention(
            embed_dim=embed_dim,
            num_heads=num_heads,
            batch_first=True,
        )
        self.query_chunk_size = 128

        self.importance_predictor = nn.Sequential(
            nn.Linear(embed_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid(),
        )

    def forward(self, points: Tensor, queries: Tensor) -> Tensor:
        """Predict per-point importance scores."""
        if queries.dim() == 1:
            queries = queries.unsqueeze(0)

        point_emb = self.point_encoder(points).unsqueeze(0)   # [1, N, D]
        query_emb = self.query_encoder(queries).unsqueeze(0)  # [1, M, D]

        per_point_context = chunked_cross_attention_context(
            self.cross_attention,
            point_emb,
            query_emb,
            self.query_chunk_size,
        )

        point_features = point_emb.squeeze(0) + per_point_context  # [N, D]
        return self.importance_predictor(point_features).squeeze(-1)
