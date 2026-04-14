"""Evaluation metrics for AIS trajectory simplification. See src/evaluation/README.md."""

from __future__ import annotations

import time
from typing import Any

import torch
from torch import Tensor

from src.queries.query_executor import run_queries


def query_error(
    original_points: Tensor,
    simplified_points: Tensor,
    queries: Tensor,
    point_chunk_size: int = 200_000,
    query_chunk_size: int | None = None,
) -> float:
    """Compute the mean relative query error between original and simplified data."""
    original_results   = run_queries(
        original_points,
        queries,
        point_chunk_size=point_chunk_size,
        query_chunk_size=query_chunk_size,
    )  # [M]
    simplified_results = run_queries(
        simplified_points,
        queries,
        point_chunk_size=point_chunk_size,
        query_chunk_size=query_chunk_size,
    )  # [M]

    denom = original_results.abs().clamp(min=1e-8)
    relative_errors = (original_results - simplified_results).abs() / denom
    return relative_errors.mean().item()


def compute_query_error(
    original_points: Tensor,
    simplified_points: Tensor,
    queries: Tensor,
    original_results: Tensor | None = None,
    point_chunk_size: int = 200_000,
    query_chunk_size: int | None = None,
) -> tuple[float, float]:
    """Compute mean and max relative query error between original and simplified data."""
    if original_results is None:
        original_results = run_queries(
            original_points,
            queries,
            point_chunk_size=point_chunk_size,
            query_chunk_size=query_chunk_size,
        )
    simplified_results = run_queries(
        simplified_points,
        queries,
        point_chunk_size=point_chunk_size,
        query_chunk_size=query_chunk_size,
    )

    denom = original_results.abs().clamp(min=1e-8)
    relative_errors = (original_results - simplified_results).abs() / denom
    return relative_errors.mean().item(), relative_errors.max().item()


def compression_ratio(
    original_points: Tensor,
    simplified_points: Tensor,
) -> float:
    """Compute the fraction of points retained: K / N."""
    n_original   = original_points.shape[0]
    n_simplified = simplified_points.shape[0]
    if n_original == 0:
        return 1.0
    return n_simplified / n_original


def compute_compression_metrics(
    original_points: Tensor,
    simplified_points: Tensor,
    trajectory_boundaries: list[tuple[int, int]],
    retained_mask: Tensor,
) -> dict[str, float | int]:
    """Compute compression summary metrics for a simplification result."""
    n_original = original_points.shape[0]
    n_simplified = simplified_points.shape[0]
    n_trajectories = len(trajectory_boundaries)

    ratio = n_simplified / n_original if n_original > 0 else 1.0
    avg_before = n_original / max(1, n_trajectories)
    avg_after = n_simplified / max(1, n_trajectories)
    n_preserved = sum(
        1 for start, end in trajectory_boundaries
        if retained_mask[start:end].any().item()
    )

    return {
        "compression_ratio": ratio,
        "avg_points_before": avg_before,
        "avg_points_after": avg_after,
        "trajectories_preserved": n_preserved,
        "n_trajectories": n_trajectories,
    }


def query_latency(
    points: Tensor,
    queries: Tensor,
    point_chunk_size: int = 200_000,
    query_chunk_size: int | None = None,
) -> float:
    """Measure the average query execution time in seconds per query."""
    start = time.perf_counter()
    run_queries(
        points,
        queries,
        point_chunk_size=point_chunk_size,
        query_chunk_size=query_chunk_size,
    )
    elapsed = time.perf_counter() - start
    n_queries = queries.shape[0]
    return elapsed / max(1, n_queries)


def compute_typed_query_error(
    original_points: Tensor,
    simplified_points: Tensor,
    typed_queries: list[dict[str, Any]],
    original_trajectories: list[Tensor] | None = None,
    simplified_trajectories: list[Tensor] | None = None,
) -> tuple[float, dict[str, float]]:
    """Compute mean query error for a list of typed queries."""
    from src.queries.query_types import execute_query

    if not typed_queries:
        return 0.0, {}

    errors_by_type: dict[str, list[float]] = {}

    for q in typed_queries:
        qtype: str = q.get("type", "unknown")
        true_val = execute_query(q, original_points, original_trajectories)
        approx_val = execute_query(q, simplified_points, simplified_trajectories)

        if qtype == "nearest":
            err = abs(true_val - approx_val)
        else:
            err = abs(true_val - approx_val) / max(abs(true_val), 1.0)

        errors_by_type.setdefault(qtype, []).append(err)

    per_type_mean: dict[str, float] = {
        qt: sum(errs) / len(errs) for qt, errs in errors_by_type.items()
    }
    all_errors = [e for errs in errors_by_type.values() for e in errs]
    mean_error = sum(all_errors) / len(all_errors)

    return mean_error, per_type_mean
