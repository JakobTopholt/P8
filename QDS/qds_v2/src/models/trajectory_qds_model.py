"""Per-trajectory transformer model for typed importance prediction. See src/models/README.md for details."""

from __future__ import annotations

import math

import torch
import torch.nn as nn

from src.models.attention_utils import chunked_cross_attention_context
from src.queries.query_types import NUM_QUERY_TYPES


class TrajectoryQDSModel(nn.Module):
    """Transformer + query cross-attention predictor. See src/models/README.md for details."""

    def __init__(
        self,
        point_dim: int,
        query_dim: int,
        embed_dim: int = 64,
        num_heads: int = 4,
        num_layers: int = 3,
        type_embed_dim: int = 16,
        query_chunk_size: int = 128,
        dropout: float = 0.1,
        use_cls_token: bool = True,
    ) -> None:
        super().__init__()
        self.point_dim = point_dim
        self.query_dim = query_dim
        self.embed_dim = embed_dim
        self.query_chunk_size = query_chunk_size
        self.use_cls_token = bool(use_cls_token)

        self.point_encoder = nn.Sequential(
            nn.Linear(point_dim, embed_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(embed_dim, embed_dim),
        )

        # Trajectory-summary CLS token. Prepended to each batch element before the
        # local transformer so global structure (overall shape, query-relevant
        # extent) is summarised into a single embedding the cross-attention can
        # consult — particularly useful for similarity / clustering query types
        # whose answer is trajectory-level, not point-level.
        if self.use_cls_token:
            self.cls_token = nn.Parameter(torch.randn(1, 1, embed_dim) * 0.02)

        enc_layer = nn.TransformerEncoderLayer(
            d_model=embed_dim,
            nhead=num_heads,
            dim_feedforward=embed_dim * 4,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
        )
        self.local_transformer = nn.TransformerEncoder(enc_layer, num_layers=num_layers)

        self.type_embedding = nn.Embedding(NUM_QUERY_TYPES, type_embed_dim)
        self.query_encoder = nn.Sequential(
            nn.Linear(query_dim + type_embed_dim, embed_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(embed_dim, embed_dim),
        )

        self.cross_attention = nn.MultiheadAttention(
            embed_dim=embed_dim,
            num_heads=num_heads,
            dropout=dropout,
            batch_first=True,
        )

        # Four independent output heads — one per query type — so each type's
        # gradient flows through its own weights and doesn't compete with the others.
        self.type_heads = nn.ModuleList([
            nn.Sequential(
                nn.Linear(embed_dim, embed_dim // 2),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(embed_dim // 2, 1),
            )
            for _ in range(NUM_QUERY_TYPES)
        ])

        # Per-head log-temperature: scales each head's logits by exp(-log_temp)
        # so the four heads can converge to compatible magnitudes during training.
        # Initialized to zero (identity at init), clamped at forward time to
        # keep gradients well-behaved if a head wants extreme calibration.
        self.head_log_temperature = nn.Parameter(torch.zeros(NUM_QUERY_TYPES))

    def _positional_encoding(self, length: int, device: torch.device) -> torch.Tensor:
        """Build sinusoidal positional encoding. See src/models/README.md for details."""
        pos = torch.arange(length, device=device, dtype=torch.float32).unsqueeze(1)
        div = torch.exp(
            torch.arange(0, self.embed_dim, 2, device=device, dtype=torch.float32)
            * (-math.log(10000.0) / self.embed_dim)
        )
        pe = torch.zeros((length, self.embed_dim), device=device, dtype=torch.float32)
        pe[:, 0::2] = torch.sin(pos * div)
        pe[:, 1::2] = torch.cos(pos * div)
        return pe

    def forward(
        self,
        points: torch.Tensor,
        queries: torch.Tensor,
        query_type_ids: torch.Tensor | None,
        padding_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Predict per-point, per-type scores. See src/models/README.md for details."""
        if query_type_ids is None:
            if self.training:
                raise RuntimeError("query_type_ids must be provided in training mode.")
            query_type_ids = torch.zeros((queries.shape[0],), dtype=torch.long, device=queries.device)

        h = self.point_encoder(points)
        h = h + self._positional_encoding(h.shape[1], h.device).unsqueeze(0)

        if self.use_cls_token and hasattr(self, "cls_token"):
            B = h.shape[0]
            cls = self.cls_token.expand(B, 1, -1)
            seq = torch.cat([cls, h], dim=1)
            if padding_mask is not None:
                cls_pad = torch.zeros((B, 1), dtype=torch.bool, device=h.device)
                seq_pad = torch.cat([cls_pad, padding_mask], dim=1)
            else:
                seq_pad = None
            seq = self.local_transformer(seq, src_key_padding_mask=seq_pad)
            cls_out = seq[:, 0:1, :]
            h = seq[:, 1:, :]
        else:
            cls_out = None
            h = self.local_transformer(h, src_key_padding_mask=padding_mask)

        q_type_emb = self.type_embedding(query_type_ids)
        q_in = torch.cat([queries, q_type_emb], dim=1)
        q_emb = self.query_encoder(q_in).unsqueeze(0).expand(h.shape[0], -1, -1)

        # When CLS is enabled, augment the cross-attention key/value space with the
        # per-trajectory summary so each point can pull from queries AND from a
        # global view of its own trajectory. The CLS embedding is per-batch-element,
        # so this introduces no cross-trajectory leakage.
        if cls_out is not None:
            q_emb = torch.cat([cls_out, q_emb], dim=1)

        context = chunked_cross_attention_context(
            self.cross_attention,
            point_features=h,
            query_features=q_emb,
            query_chunk_size=self.query_chunk_size,
            point_padding_mask=padding_mask,
        )

        logits = torch.cat(
            [head(h + context) for head in self.type_heads], dim=-1
        )
        if hasattr(self, "head_log_temperature"):
            temp = self.head_log_temperature.clamp(min=-2.3, max=2.3).exp()  # ~[0.1, 10]
            logits = logits / temp.view(1, 1, -1)
        return logits


def normalize_points_and_queries(
    points: torch.Tensor,
    queries: torch.Tensor,
    point_min: torch.Tensor,
    point_max: torch.Tensor,
    query_min: torch.Tensor,
    query_max: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Min-max normalize points and queries with persisted stats. See src/models/README.md for details."""
    eps = 1e-6
    p = (points - point_min) / torch.clamp(point_max - point_min, min=eps)
    q = (queries - query_min) / torch.clamp(query_max - query_min, min=eps)
    return p, q
