"""Baseline QDS model for AIS trajectories. See src/models/README.md for architecture details."""

from __future__ import annotations

import torch
import torch.nn as nn
from torch import Tensor

from src.models.attention_utils import chunked_cross_attention_context

# ---------------------------------------------------------------------------
# Query-type integer identifiers — keep in sync with query_types.py
# ---------------------------------------------------------------------------
QUERY_TYPE_ID_RANGE        = 0  # range: spatiotemporal speed-sum
QUERY_TYPE_ID_INTERSECTION = 1  # intersection: trajectory count
QUERY_TYPE_ID_AGGREGATION  = 2  # aggregation: data-point count
QUERY_TYPE_ID_NEAREST      = 3  # nearest: mean kNN distance

NUM_QUERY_TYPES = 4


def normalize_points_and_queries(
    points: Tensor,
    queries: Tensor,
    query_type_ids: Tensor | None = None,
) -> tuple[Tensor, Tensor]:
    """Min-max normalise points and queries using point-cloud feature ranges.

    The ``query_type_ids`` argument is accepted for API symmetry with the model
    forward pass but is not used numerically; type information is handled by the
    learned embedding inside :class:`TrajectoryQDSModel`.
    """
    norm_points = points.clone()
    norm_queries = queries.clone()

    eps = torch.tensor(1e-8, dtype=points.dtype, device=points.device)

    # Normalise only the first 5 features (time, lat, lon, speed, heading).
    # Columns 5+ (is_start, is_end, turn_score, boundary_proximity, …) are
    # already in [0, 1] and are passed through unchanged.
    n_spatial_features = min(5, points.shape[1])
    p_min = points[:, :n_spatial_features].min(dim=0).values
    p_max = points[:, :n_spatial_features].max(dim=0).values
    p_range = torch.maximum(p_max - p_min, eps)

    norm_points[:, :n_spatial_features] = (
        (norm_points[:, :n_spatial_features] - p_min) / p_range
    ).clamp(0.0, 1.0)

    # Query lat bounds use point lat scale
    norm_queries[:, 0] = (norm_queries[:, 0] - p_min[1]) / p_range[1]  # lat_min
    norm_queries[:, 1] = (norm_queries[:, 1] - p_min[1]) / p_range[1]  # lat_max

    # Query lon bounds use point lon scale
    norm_queries[:, 2] = (norm_queries[:, 2] - p_min[2]) / p_range[2]  # lon_min
    norm_queries[:, 3] = (norm_queries[:, 3] - p_min[2]) / p_range[2]  # lon_max

    # Query time bounds use point time scale
    norm_queries[:, 4] = (norm_queries[:, 4] - p_min[0]) / p_range[0]  # time_start
    norm_queries[:, 5] = (norm_queries[:, 5] - p_min[0]) / p_range[0]  # time_end

    norm_queries = norm_queries.clamp(0.0, 1.0)

    return norm_points, norm_queries


class TrajectoryQDSModel(nn.Module):
    """Query-Driven Simplification model for AIS trajectory data.

    Extends the baseline cross-attention architecture with query-type awareness
    and point self-attention:

    - **Query-type embedding** — a learned vector per query type (range /
      intersection / aggregation / nearest) is concatenated to the 6 numeric
      query features before encoding.  This lets the model learn type-specific
      importance signals across the four supported query semantics.

    - **Point self-attention** — a lightweight self-attention layer lets each
      point attend to its trajectory neighbours before cross-attention with
      queries, capturing neighbourhood context (e.g. "this point precedes a
      sharp turn").

    - **LayerNorm** — applied after every attention block for stable training
      on variable-length AIS trajectories.

    Forward inputs
    --------------
    points        : [N, 7]  float — (time, lat, lon, speed, heading, is_start, is_end)
    queries       : [M, 6]  float — (lat_min, lat_max, lon_min, lon_max, t_start, t_end)
    query_type_ids: [M]     long  — integer type label per query (optional; defaults
                                    to ``QUERY_TYPE_ID_RANGE`` for all queries)

    Forward output
    --------------
    scores : [N] float in (0, 1) — per-point importance
    """

    def __init__(
        self,
        embed_dim: int = 64,
        num_heads: int = 4,
        num_query_types: int = NUM_QUERY_TYPES,
        type_embed_dim: int = 16,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()

        # --- Query-type embedding: one learned vector per query type,
        #     concatenated to the 6 numeric query features before encoding ---
        self.query_type_embed = nn.Embedding(num_query_types, type_embed_dim)
        query_in_dim = 6 + type_embed_dim

        # --- Point encoder: [time, lat, lon, speed, heading, is_start, is_end] → embed_dim ---
        self.point_encoder = nn.Sequential(
            nn.Linear(7, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )

        # --- Point self-attention + norm: each point attends to its trajectory
        #     neighbours before query interaction to capture local context ---
        self.point_self_attn = nn.MultiheadAttention(
            embed_dim=embed_dim,
            num_heads=num_heads,
            batch_first=True,
            dropout=dropout,
        )
        self.point_norm = nn.LayerNorm(embed_dim)

        # --- Query encoder: [lat_min, lat_max, lon_min, lon_max, t_start, t_end,
        #     <type_embedding>] → embed_dim ---
        self.query_encoder = nn.Sequential(
            nn.Linear(query_in_dim, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )

        # --- Cross-attention (queries attend to points) + norm ---
        self.cross_attention = nn.MultiheadAttention(
            embed_dim=embed_dim,
            num_heads=num_heads,
            batch_first=True,
            dropout=dropout,
        )
        self.cross_norm = nn.LayerNorm(embed_dim)
        self.query_chunk_size = 128

        # --- Importance predictor ---
        self.importance_predictor = nn.Sequential(
            nn.Linear(embed_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid(),
        )

    def forward(
        self,
        points: Tensor,
        queries: Tensor,
        query_type_ids: Tensor | None = None,
    ) -> Tensor:
        """Predict per-point importance scores."""
        # Ensure queries has a batch-compatible shape; handle single-query case
        if queries.dim() == 1:
            queries = queries.unsqueeze(0)
            if query_type_ids is not None:
                query_type_ids = query_type_ids.unsqueeze(0)

        # Default all type IDs to RANGE (0) when not supplied
        if query_type_ids is None:
            query_type_ids = queries.new_zeros(queries.shape[0], dtype=torch.long)

        # --- Encode points ---
        point_emb = self.point_encoder(points).unsqueeze(0)          # [1, N, D]

        # Self-attention: each point gathers context from its trajectory neighbours
        sa_out, _ = self.point_self_attn(
            point_emb,
            point_emb,
            point_emb,
            need_weights=False,
        )
        point_emb = self.point_norm(point_emb + sa_out)              # residual + norm

        # --- Encode queries (with type embedding) ---
        type_emb  = self.query_type_embed(query_type_ids)            # [M, type_embed_dim]
        query_in  = torch.cat([queries, type_emb], dim=-1)           # [M, 6 + type_embed_dim]
        query_emb = self.query_encoder(query_in).unsqueeze(0)        # [1, M, D]

        # Queries (Q) attend to points (K, V); accumulate contexts in chunks.
        per_point_context = chunked_cross_attention_context(
            self.cross_attention,
            point_emb,
            query_emb,
            self.query_chunk_size,
            self.cross_norm,
        )

        point_features = point_emb.squeeze(0) + per_point_context    # [N, D]
        scores = self.importance_predictor(point_features).squeeze(-1)
        return scores
