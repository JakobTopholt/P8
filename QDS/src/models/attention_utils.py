"""Attention helper utilities for query-conditioned trajectory encoding. See src/models/README.md for details."""

from __future__ import annotations

import torch
import torch.nn as nn
from torch import Tensor


def chunked_cross_attention_context(
    cross_attention: nn.MultiheadAttention,
    point_features: Tensor,
    query_features: Tensor,
    query_chunk_size: int,
    point_padding_mask: Tensor | None = None,
) -> Tensor:
    """Compute per-point query context with points attending to queries. See src/models/README.md for details.

    Normalisation note: a full attention call already returns a softmax-weighted
    average over its key/value queries, so a single chunk must not be divided by
    the query count again. When chunking is needed, chunk outputs are averaged
    by chunk count as a bounded approximation.

    Direction: ``point_features`` acts as Q and ``query_features`` as K,V.
    Each point attends over all query embeddings to produce a
    query-conditioned context vector.  This is the correct direction for
    per-point query context: a point inside a range-query box will receive
    high attention from range-query embeddings and low attention from others,
    giving the per-type head a clear conditioning signal.

    Chunking splits the key/value (query) space.  Because softmax is applied
    independently inside each chunk, this is an approximation; if the chunk
    size is at least the number of queries the entire set fits in a single
    chunk and the result is exact.  Default chunk size (4096) covers all
    realistic workloads (kNN-only caps at 4000 queries), so chunking is
    effectively never triggered in normal use.
    """
    if point_features.dim() != 3 or query_features.dim() != 3:
        raise ValueError("point_features and query_features must be [B,L,D] and [B,M,D].")

    batch, length, embed = point_features.shape
    n_queries = int(query_features.shape[1])
    if n_queries == 0:
        return point_features.new_zeros((batch, length, embed))

    context = point_features.new_zeros((batch, length, embed))
    step = max(1, int(query_chunk_size))
    chunk_count = 0

    for start in range(0, n_queries, step):
        end = min(n_queries, start + step)
        q_chunk = query_features[:, start:end, :]
        # Points (Q) attend to the current query chunk (K, V).
        # Padded point positions (padding_mask True) are still allowed to
        # produce output here; their predictions are excluded from the loss
        # via the valid_mask in the training loop.
        attn_out, _ = cross_attention(
            query=point_features,
            key=q_chunk,
            value=q_chunk,
        )
        context = context + attn_out
        chunk_count += 1

    return context / float(max(1, chunk_count))
