"""Attention helpers for QDS models."""

from __future__ import annotations

import torch
import torch.nn as nn
from torch import Tensor


def chunked_cross_attention_context(
    cross_attention: nn.MultiheadAttention,
    point_emb: Tensor,
    query_emb: Tensor,
    query_chunk_size: int,
    post_attention_norm: nn.Module | None = None,
) -> Tensor:
    """Compute per-point query context without materializing all query weights."""
    if point_emb.dim() != 3 or query_emb.dim() != 3:
        raise ValueError("point_emb and query_emb must have shape [1, N, D] and [1, M, D].")

    if point_emb.shape[0] != 1 or query_emb.shape[0] != 1:
        raise ValueError("chunked_cross_attention_context expects batch size 1 tensors.")

    n_points = point_emb.shape[1]
    embed_dim = point_emb.shape[2]
    n_queries = query_emb.shape[1]
    if n_queries == 0:
        return point_emb.new_zeros((n_points, embed_dim))

    chunk_size = max(1, int(query_chunk_size))
    per_point_context = point_emb.new_zeros((n_points, embed_dim))

    for start in range(0, n_queries, chunk_size):
        end = min(n_queries, start + chunk_size)
        query_chunk = query_emb[:, start:end]
        attn_out, attn_weights = cross_attention(
            query=query_chunk,
            key=point_emb,
            value=point_emb,
        )
        if post_attention_norm is not None:
            attn_out = post_attention_norm(query_chunk + attn_out)
        per_point_context = per_point_context + torch.mm(
            attn_weights.squeeze(0).transpose(0, 1),
            attn_out.squeeze(0),
        )

    return per_point_context