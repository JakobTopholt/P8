"""Ground-truth importance label computation. See src/training/README.md for algorithm details."""

from __future__ import annotations

from typing import Optional

import torch
from torch import Tensor


def compute_importance(
    points: Tensor,
    queries: Tensor,
    sample_points: Optional[int] = None,
    chunk_size: int = 200_000,
) -> Tensor:
    """Compute normalised leave-one-out importance scores for trajectory points."""
    n_points = points.shape[0]
    n_queries = queries.shape[0]
    importance = torch.zeros(n_points, dtype=torch.float32, device=points.device)

    if n_points == 0 or n_queries == 0:
        return importance

    chunk_size = max(1, int(chunk_size))

    # Pass 1: compute full query results in chunks to avoid [N, M] allocation.
    query_results = torch.zeros(n_queries, dtype=torch.float32, device=points.device)
    for start in range(0, n_points, chunk_size):
        end = min(n_points, start + chunk_size)
        chunk = points[start:end]

        inclusion = (
            (chunk[:, None, 1] >= queries[None, :, 0]) &
            (chunk[:, None, 1] <= queries[None, :, 1]) &
            (chunk[:, None, 2] >= queries[None, :, 2]) &
            (chunk[:, None, 2] <= queries[None, :, 3]) &
            (chunk[:, None, 0] >= queries[None, :, 4]) &
            (chunk[:, None, 0] <= queries[None, :, 5])
        )

        speed_chunk = chunk[:, 3].abs().unsqueeze(1)  # [C, 1]
        query_results += (inclusion.float() * speed_chunk).sum(dim=0)

    denom = query_results.abs()
    safe_denom = torch.where(denom > 1e-8, denom, torch.ones_like(denom))

    # Pass 2: compute per-point importance in chunks.
    for start in range(0, n_points, chunk_size):
        end = min(n_points, start + chunk_size)
        chunk = points[start:end]

        inclusion = (
            (chunk[:, None, 1] >= queries[None, :, 0]) &
            (chunk[:, None, 1] <= queries[None, :, 1]) &
            (chunk[:, None, 2] >= queries[None, :, 2]) &
            (chunk[:, None, 2] <= queries[None, :, 3]) &
            (chunk[:, None, 0] >= queries[None, :, 4]) &
            (chunk[:, None, 0] <= queries[None, :, 5])
        )

        speed_chunk = chunk[:, 3].abs().unsqueeze(1)  # [C, 1]

        # For positive-speed workloads this equals leave-one-out relative error:
        # rel_diff(i, j) = I(i in q_j) * speed_i / |query_result_j|
        relative_diff = torch.where(
            denom.unsqueeze(0) > 1e-8,
            inclusion.float() * speed_chunk / safe_denom.unsqueeze(0),
            torch.zeros((end - start, n_queries), dtype=torch.float32, device=points.device),
        )

        importance[start:end] = relative_diff.mean(dim=1)

    # Optional sub-sampling behavior: keep only sampled indices, set rest to 0.
    if sample_points is not None and sample_points < n_points:
        sampled = torch.randperm(n_points, device=points.device)[:sample_points]
        sampled_mask = torch.zeros(n_points, dtype=torch.bool, device=points.device)
        sampled_mask[sampled] = True
        importance = torch.where(sampled_mask, importance, torch.zeros_like(importance))

    # Normalise importance scores to [0, 1]
    min_val = importance.min()
    max_val = importance.max()
    if (max_val - min_val).item() > 1e-8:
        importance = (importance - min_val) / (max_val - min_val)
    else:
        importance = torch.zeros_like(importance)

    return importance
