"""Tests query cross-attention context scaling."""

from __future__ import annotations

import torch
import torch.nn as nn

from src.models.attention_utils import chunked_cross_attention_context


def test_single_chunk_attention_matches_direct_multihead_attention() -> None:
    """Assert helper does not divide a full attention call by query count."""
    torch.manual_seed(123)
    attention = nn.MultiheadAttention(embed_dim=8, num_heads=2, batch_first=True, dropout=0.0)
    points = torch.randn((1, 3, 8))
    queries = torch.randn((1, 5, 8))

    expected, _ = attention(query=points, key=queries, value=queries)
    actual = chunked_cross_attention_context(
        attention,
        point_features=points,
        query_features=queries,
        query_chunk_size=128,
    )

    assert torch.allclose(actual, expected, atol=1e-6)