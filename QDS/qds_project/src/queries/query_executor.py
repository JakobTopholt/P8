"""Spatiotemporal range query execution against AIS point clouds. See src/queries/README.md."""

from __future__ import annotations

import torch
from torch import Tensor


def run_query(points: Tensor, query: Tensor) -> Tensor:
    """Run a single spatiotemporal range query; returns sum of speed for matching points."""
    lat_min, lat_max, lon_min, lon_max, time_start, time_end = (
        query[0], query[1], query[2], query[3], query[4], query[5],
    )

    mask = (
        (points[:, 1] >= lat_min)  & (points[:, 1] <= lat_max) &
        (points[:, 2] >= lon_min)  & (points[:, 2] <= lon_max) &
        (points[:, 0] >= time_start) & (points[:, 0] <= time_end)
    )

    return points[mask, 3].sum()


def run_queries(
    points: Tensor,
    queries: Tensor,
    point_chunk_size: int = 200_000,
    query_chunk_size: int | None = None,
) -> Tensor:
    """Run all M queries in a chunked manner against the point cloud."""
    n_points = points.shape[0]
    n_queries = queries.shape[0]

    if n_points == 0 or n_queries == 0:
        return torch.zeros(n_queries, dtype=points.dtype, device=points.device)

    # Chunk across both dimensions to keep temporary [P, Q] masks small when
    # workloads contain many queries.
    p_chunk = max(1, int(point_chunk_size))
    if query_chunk_size is None:
        q_chunk = n_queries if n_queries <= 128 else 128
    else:
        q_chunk = max(1, int(query_chunk_size))
    results = torch.zeros(n_queries, dtype=points.dtype, device=points.device)

    for q_start in range(0, n_queries, q_chunk):
        q_end = min(n_queries, q_start + q_chunk)
        query_chunk = queries[q_start:q_end]
        chunk_results = torch.zeros(q_end - q_start, dtype=points.dtype, device=points.device)

        for p_start in range(0, n_points, p_chunk):
            p_end = min(n_points, p_start + p_chunk)
            chunk = points[p_start:p_end]

            mask = (
                (chunk[:, None, 1] >= query_chunk[None, :, 0]) &
                (chunk[:, None, 1] <= query_chunk[None, :, 1]) &
                (chunk[:, None, 2] >= query_chunk[None, :, 2]) &
                (chunk[:, None, 2] <= query_chunk[None, :, 3]) &
                (chunk[:, None, 0] >= query_chunk[None, :, 4]) &
                (chunk[:, None, 0] <= query_chunk[None, :, 5])
            )

            chunk_results += (mask.float() * chunk[:, 3].unsqueeze(1)).sum(dim=0)

        results[q_start:q_end] = chunk_results

    return results
