"""Typed query representation and dispatcher for multi-type query workloads.

Queries are represented as plain dicts with a ``"type"`` key and a ``"params"``
dict.  The dispatcher :func:`execute_query` routes to the appropriate executor
based on the type string.

Supported types
---------------
range
    Spatiotemporal range query; returns the sum of speed values for all points
    inside the region (matches the existing :func:`run_queries` semantics).
intersection
    Returns the number of trajectories that have at least one point inside the
    spatiotemporal region.  Falls back to a point-level count when ``trajectories``
    is not supplied.
aggregation
    Returns the number of data-points inside the spatiotemporal region.
nearest
    k-Nearest-Neighbour (kNN) query.  Returns the **mean Euclidean distance**
    (in lat/lon degrees) to the *k* closest AIS points within the configured
    time window.  When ``k=1`` (the default) this is identical to the original
    single-NN behaviour.  Distance is computed as:

        distance = sqrt((lat - query_lat)^2 + (lon - query_lon)^2)

    Query params:
        query_lat (float): latitude of the query point.
        query_lon (float): longitude of the query point.
        query_time (float): centre of the time window.
        time_window (float, default 1.0): half-width of the time window.
        k (int, default 1): number of nearest neighbours to consider.
"""

from __future__ import annotations

import math
from typing import Any

import torch
from torch import Tensor

QUERY_TYPE_RANGE = "range"
QUERY_TYPE_INTERSECTION = "intersection"
QUERY_TYPE_AGGREGATION = "aggregation"
QUERY_TYPE_NEAREST = "nearest"

VALID_QUERY_TYPES = frozenset(
    {QUERY_TYPE_RANGE, QUERY_TYPE_INTERSECTION, QUERY_TYPE_AGGREGATION, QUERY_TYPE_NEAREST}
)


# ---------------------------------------------------------------------------
# Public dispatcher
# ---------------------------------------------------------------------------


def execute_query(
    query: dict[str, Any],
    points: Tensor,
    trajectories: list[Tensor] | None = None,
) -> float:
    """Execute a single typed query and return a scalar result.

    Args:
        query: Dict with keys ``"type"`` (str) and ``"params"`` (dict).
        points: Flat AIS point tensor [N, F] with columns
            ``[time, lat, lon, speed, ...]``.
        trajectories: Optional list of per-ship tensors used by intersection
            queries to count intersecting trajectories.  If *None*, intersection
            queries fall back to counting matching points.

    Returns:
        A non-negative float whose semantics depend on the query type.

    Raises:
        ValueError: If the query type is unknown.
    """
    qtype = query.get("type")
    params = query.get("params", {})

    if qtype == QUERY_TYPE_RANGE:
        return _run_range_query(points, params)
    if qtype == QUERY_TYPE_INTERSECTION:
        return _run_intersection_query(points, params, trajectories)
    if qtype == QUERY_TYPE_AGGREGATION:
        return _run_aggregation_query(points, params)
    if qtype == QUERY_TYPE_NEAREST:
        return _run_nearest_neighbor_query(points, params)

    raise ValueError(
        f"Unknown query type '{qtype}'. Valid types: {sorted(VALID_QUERY_TYPES)}"
    )


def execute_typed_queries(
    queries: list[dict[str, Any]],
    points: Tensor,
    trajectories: list[Tensor] | None = None,
) -> list[float]:
    """Execute a list of typed queries and return a list of scalar results.

    Args:
        queries: List of typed query dicts.
        points: Flat AIS point tensor [N, F].
        trajectories: Optional list of per-ship tensors (used by intersection).

    Returns:
        List of floats, one per query.
    """
    return [execute_query(q, points, trajectories) for q in queries]


# ---------------------------------------------------------------------------
# Individual executors
# ---------------------------------------------------------------------------


def _run_range_query(points: Tensor, params: dict[str, Any]) -> float:
    """Range query: returns sum of speed for matching points."""
    from src.queries.query_executor import run_query

    query_tensor = torch.tensor(
        [
            params["lat_min"],
            params["lat_max"],
            params["lon_min"],
            params["lon_max"],
            params["time_start"],
            params["time_end"],
        ],
        dtype=points.dtype,
        device=points.device,
    )
    return float(run_query(points, query_tensor).item())


def _run_intersection_query(
    points: Tensor,
    params: dict[str, Any],
    trajectories: list[Tensor] | None,
) -> float:
    """Intersection query: count of trajectories (or points) inside the region.

    When *trajectories* is provided the result is the number of ships that have
    at least one point satisfying all spatiotemporal constraints.  When only
    *points* is available (e.g. when evaluating simplified data without explicit
    trajectory structure) the result is the number of matching points, which is
    consistent with the aggregation semantics and allows relative-error
    evaluation.
    """
    lat_min = params["lat_min"]
    lat_max = params["lat_max"]
    lon_min = params["lon_min"]
    lon_max = params["lon_max"]
    time_start = params["time_start"]
    time_end = params["time_end"]

    if trajectories is not None:
        count = 0
        for traj in trajectories:
            mask = (
                (traj[:, 1] >= lat_min)
                & (traj[:, 1] <= lat_max)
                & (traj[:, 2] >= lon_min)
                & (traj[:, 2] <= lon_max)
                & (traj[:, 0] >= time_start)
                & (traj[:, 0] <= time_end)
            )
            if mask.any():
                count += 1
        return float(count)

    # Fallback: count matching points
    mask = (
        (points[:, 1] >= lat_min)
        & (points[:, 1] <= lat_max)
        & (points[:, 2] >= lon_min)
        & (points[:, 2] <= lon_max)
        & (points[:, 0] >= time_start)
        & (points[:, 0] <= time_end)
    )
    return float(mask.sum().item())


def _run_aggregation_query(points: Tensor, params: dict[str, Any]) -> float:
    """Aggregation query: count of data-points inside the spatiotemporal region."""
    lat_min = params["lat_min"]
    lat_max = params["lat_max"]
    lon_min = params["lon_min"]
    lon_max = params["lon_max"]
    time_start = params["time_start"]
    time_end = params["time_end"]

    mask = (
        (points[:, 1] >= lat_min)
        & (points[:, 1] <= lat_max)
        & (points[:, 2] >= lon_min)
        & (points[:, 2] <= lon_max)
        & (points[:, 0] >= time_start)
        & (points[:, 0] <= time_end)
    )
    return float(mask.sum().item())


def _run_nearest_neighbor_query(points: Tensor, params: dict[str, Any]) -> float:
    """kNN query: mean Euclidean distance to the *k* closest AIS points.

    Distance is the Euclidean distance in lat/lon degrees:
    ``sqrt((lat - query_lat)^2 + (lon - query_lon)^2)``.

    Only points whose timestamp is within ``[query_time - time_window,
    query_time + time_window]`` are considered.  If no points fall within the
    window the search widens automatically to the full dataset so a result is
    always returned (unless the dataset is empty).

    When ``k=1`` (the default) the result equals the distance to the single
    closest point, preserving full backward compatibility.

    Args:
        points: AIS point tensor [N, F] with columns ``[time, lat, lon, ...]``.
        params: Dict with keys:
            ``query_lat`` (float), ``query_lon`` (float),
            ``query_time`` (float), ``time_window`` (float, default 1.0),
            ``k`` (int, default 1).

    Returns:
        Mean distance to the *k* nearest neighbours as a Python float, or
        ``math.inf`` if *points* is empty.
    """
    query_lat = params["query_lat"]
    query_lon = params["query_lon"]
    query_time = params["query_time"]
    time_window = params.get("time_window", 1.0)
    k = int(params.get("k", 1))

    mask = (
        (points[:, 0] >= query_time - time_window)
        & (points[:, 0] <= query_time + time_window)
    )
    nearby = points[mask]

    if nearby.shape[0] == 0:
        nearby = points  # Widen to all points

    if nearby.shape[0] == 0:
        return math.inf

    dlat = nearby[:, 1] - query_lat
    dlon = nearby[:, 2] - query_lon
    distances = torch.sqrt(dlat * dlat + dlon * dlon)

    # Clamp k to the number of available points
    effective_k = min(k, distances.shape[0])
    if effective_k == 1:
        return float(distances.min().item())

    topk_dists = torch.topk(distances, k=effective_k, largest=False).values
    return float(topk_dists.mean().item())
