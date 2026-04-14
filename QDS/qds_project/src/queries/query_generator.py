"""Spatiotemporal query generation. See src/queries/README.md for strategies."""

from __future__ import annotations

from typing import Any, List

import torch
from torch import Tensor


def _compute_bounds(
    all_points: Tensor,
) -> tuple[Tensor, Tensor, Tensor, Tensor, Tensor, Tensor]:
    """Return (time_min, time_max, lat_min, lat_max, lon_min, lon_max) from a flat point cloud tensor."""
    time_min = all_points[:, 0].min()
    time_max = all_points[:, 0].max()
    lat_min  = all_points[:, 1].min()
    lat_max  = all_points[:, 1].max()
    lon_min  = all_points[:, 2].min()
    lon_max  = all_points[:, 2].max()
    return time_min, time_max, lat_min, lat_max, lon_min, lon_max


def _guard_range(r: Tensor) -> Tensor:
    """Return *r* unchanged if it is meaningfully positive, else 1.0."""
    return r if r > 1e-6 else torch.tensor(1.0)


def _effective_spatial_ranges(
    all_points: Tensor,
    lat_range: Tensor,
    lon_range: Tensor,
) -> tuple[Tensor, Tensor]:
    """Return robust spatial ranges for query width sampling using 5th–95th percentiles."""
    q = torch.tensor([0.05, 0.95], dtype=all_points.dtype, device=all_points.device)
    lat_q = torch.quantile(all_points[:, 1], q)
    lon_q = torch.quantile(all_points[:, 2], q)

    robust_lat_range = _guard_range(lat_q[1] - lat_q[0])
    robust_lon_range = _guard_range(lon_q[1] - lon_q[0])

    scale = torch.tensor(1.5, dtype=all_points.dtype, device=all_points.device)
    eff_lat_range = _guard_range(torch.minimum(lat_range, robust_lat_range * scale))
    eff_lon_range = _guard_range(torch.minimum(lon_range, robust_lon_range * scale))

    return eff_lat_range, eff_lon_range


def _effective_spatial_bounds(
    all_points: Tensor,
    lower_q: float = 0.01,
    upper_q: float = 0.99,
) -> tuple[Tensor, Tensor, Tensor, Tensor]:
    """Return robust spatial bounds for center sampling using quantile clipping."""
    if not (0.0 <= lower_q < upper_q <= 1.0):
        raise ValueError(
            f"Invalid quantile bounds: lower_q={lower_q}, upper_q={upper_q}."
        )

    q = torch.tensor([lower_q, upper_q], dtype=all_points.dtype, device=all_points.device)
    lat_q = torch.quantile(all_points[:, 1], q)
    lon_q = torch.quantile(all_points[:, 2], q)

    lat_lo = lat_q[0]
    lat_hi = lat_q[1]
    lon_lo = lon_q[0]
    lon_hi = lon_q[1]

    # Fallback to strict min/max if quantile span is degenerate.
    if bool(lat_hi - lat_lo <= 1e-6):
        lat_lo = all_points[:, 1].min()
        lat_hi = all_points[:, 1].max()
    if bool(lon_hi - lon_lo <= 1e-6):
        lon_lo = all_points[:, 2].min()
        lon_hi = all_points[:, 2].max()

    return lat_lo, lat_hi, lon_lo, lon_hi


def _ensure_strict_order_within_bounds(
    q_min: Tensor,
    q_max: Tensor,
    bound_min: Tensor,
    bound_max: Tensor,
    eps: float = 1e-4,
) -> tuple[Tensor, Tensor]:
    """Ensure q_min < q_max while keeping both values inside bounds."""
    eps_t = torch.tensor(eps, dtype=q_min.dtype, device=q_min.device)
    span = bound_max - bound_min

    # If the global span is tiny, best effort is to stay inside bounds.
    if bool(span <= eps_t):
        return bound_min.expand_as(q_min), bound_max.expand_as(q_max)

    too_small = q_max <= q_min
    if too_small.any():
        upper_room = q_min + eps_t <= bound_max
        lower_room = q_max - eps_t >= bound_min

        new_max = torch.where(upper_room, q_min + eps_t, q_max)
        new_min = torch.where(~upper_room & lower_room, q_max - eps_t, q_min)

        q_min = torch.where(too_small, new_min, q_min)
        q_max = torch.where(too_small, new_max, q_max)

    q_min = torch.clamp(q_min, min=bound_min, max=bound_max)
    q_max = torch.clamp(q_max, min=bound_min, max=bound_max)

    return q_min, q_max


def _build_queries(
    centers_lat: Tensor,
    centers_lon: Tensor,
    centers_time: Tensor,
    lat_width_range: Tensor,
    lon_width_range: Tensor,
    time_range: Tensor,
    lat_min: Tensor,
    lat_max: Tensor,
    lon_min: Tensor,
    lon_max: Tensor,
    time_min: Tensor,
    time_max: Tensor,
    spatial_fraction: float,
    temporal_fraction: float,
) -> Tensor:
    """Sample query half-widths, clamp to data bounds, and assemble [M, 6] query tensor."""
    n_queries = centers_lat.shape[0]

    if spatial_fraction <= 0.0:
        raise ValueError(f"spatial_fraction must be > 0, got {spatial_fraction}.")
    if temporal_fraction <= 0.0:
        raise ValueError(f"temporal_fraction must be > 0, got {temporal_fraction}.")

    # Keep a small lower bound so boxes do not collapse to near-zero size.
    min_spatial_fraction = min(0.0025, spatial_fraction)
    min_temporal_fraction = min(0.01, temporal_fraction)

    spatial_f = (
        torch.rand(n_queries) * (spatial_fraction - min_spatial_fraction)
        + min_spatial_fraction
    )
    temporal_f = (
        torch.rand(n_queries) * (temporal_fraction - min_temporal_fraction)
        + min_temporal_fraction
    )

    # Sample half-widths (always positive, proportional to effective ranges)
    hw_lat = spatial_f * lat_width_range / 2.0
    hw_lon = spatial_f * lon_width_range / 2.0
    hw_time = temporal_f * time_range / 2.0

    # Build bounds and clamp to data range
    q_lat_min  = torch.clamp(centers_lat  - hw_lat,  min=lat_min,  max=lat_max)
    q_lat_max  = torch.clamp(centers_lat  + hw_lat,  min=lat_min,  max=lat_max)
    q_lon_min  = torch.clamp(centers_lon  - hw_lon,  min=lon_min,  max=lon_max)
    q_lon_max  = torch.clamp(centers_lon  + hw_lon,  min=lon_min,  max=lon_max)
    q_time_min = torch.clamp(centers_time - hw_time, min=time_min, max=time_max)
    q_time_max = torch.clamp(centers_time + hw_time, min=time_min, max=time_max)

    # Ensure min < max (may become equal after clamping at boundary), while
    # staying strictly inside dataset bounds.
    q_lat_min, q_lat_max = _ensure_strict_order_within_bounds(
        q_lat_min,
        q_lat_max,
        lat_min,
        lat_max,
    )
    q_lon_min, q_lon_max = _ensure_strict_order_within_bounds(
        q_lon_min,
        q_lon_max,
        lon_min,
        lon_max,
    )
    q_time_min, q_time_max = _ensure_strict_order_within_bounds(
        q_time_min,
        q_time_max,
        time_min,
        time_max,
    )

    return torch.stack(
        [q_lat_min, q_lat_max, q_lon_min, q_lon_max, q_time_min, q_time_max],
        dim=1,
    )  # [M, 6]


def generate_uniform_queries(
    trajectories: List[Tensor],
    n_queries: int = 100,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
    spatial_bound_lower_quantile: float = 0.01,
    spatial_bound_upper_quantile: float = 0.99,
) -> Tensor:
    """Generate spatiotemporal queries with centres sampled uniformly from the bounding box."""
    all_points = torch.cat(trajectories, dim=0)  # [N, 5]
    time_min, time_max, _, _, _, _ = _compute_bounds(all_points)

    lat_min, lat_max, lon_min, lon_max = _effective_spatial_bounds(
        all_points,
        lower_q=spatial_bound_lower_quantile,
        upper_q=spatial_bound_upper_quantile,
    )

    lat_range = _guard_range(lat_max - lat_min)
    lon_range = _guard_range(lon_max - lon_min)
    time_range = _guard_range(time_max - time_min)
    lat_width_range, lon_width_range = _effective_spatial_ranges(
        all_points,
        lat_range,
        lon_range,
    )

    # Uniform centre sampling from the bounding box
    centers_lat  = lat_min  + torch.rand(n_queries) * lat_range
    centers_lon  = lon_min  + torch.rand(n_queries) * lon_range
    centers_time = time_min + torch.rand(n_queries) * time_range

    return _build_queries(
        centers_lat, centers_lon, centers_time,
        lat_width_range, lon_width_range, time_range,
        lat_min, lat_max, lon_min, lon_max, time_min, time_max,
        spatial_fraction, temporal_fraction,
    )


def generate_density_biased_queries(
    trajectories: List[Tensor],
    n_queries: int = 100,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
) -> Tensor:
    """Generate spatiotemporal queries centred on real AIS data points."""
    all_points = torch.cat(trajectories, dim=0)  # [N, 5]
    time_min, time_max, lat_min, lat_max, lon_min, lon_max = _compute_bounds(all_points)

    lat_range = _guard_range(lat_max - lat_min)
    lon_range = _guard_range(lon_max - lon_min)
    time_range = _guard_range(time_max - time_min)
    lat_width_range, lon_width_range = _effective_spatial_ranges(
        all_points,
        lat_range,
        lon_range,
    )

    # Density-biased centre sampling: anchor to real AIS data points so that
    # queries are concentrated in high-traffic regions.
    anchor_idx   = torch.randint(0, all_points.shape[0], (n_queries,))
    anchors      = all_points[anchor_idx]  # [M, 5]
    centers_lat  = anchors[:, 1]
    centers_lon  = anchors[:, 2]
    # Time window centre is still drawn uniformly to allow temporal variety.
    centers_time = time_min + torch.rand(n_queries) * time_range

    return _build_queries(
        centers_lat, centers_lon, centers_time,
        lat_width_range, lon_width_range, time_range,
        lat_min, lat_max, lon_min, lon_max, time_min, time_max,
        spatial_fraction, temporal_fraction,
    )


def generate_mixed_queries(
    trajectories: List[Tensor],
    total_queries: int = 100,
    density_ratio: float = 0.5,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
    spatial_bound_lower_quantile: float = 0.01,
    spatial_bound_upper_quantile: float = 0.99,
) -> Tensor:
    """Generate a mixed workload of uniform and density-biased queries."""
    if not (0.0 <= density_ratio <= 1.0):
        raise ValueError(f"density_ratio must be in [0, 1], got {density_ratio}.")

    n_density = int(total_queries * density_ratio)
    n_uniform = total_queries - n_density

    parts: list[Tensor] = []

    if n_density > 0:
        parts.append(
            generate_density_biased_queries(
                trajectories,
                n_queries=n_density,
                spatial_fraction=spatial_fraction,
                temporal_fraction=temporal_fraction,
            )
        )
    if n_uniform > 0:
        parts.append(
            generate_uniform_queries(
                trajectories,
                n_queries=n_uniform,
                spatial_fraction=spatial_fraction,
                temporal_fraction=temporal_fraction,
                spatial_bound_lower_quantile=spatial_bound_lower_quantile,
                spatial_bound_upper_quantile=spatial_bound_upper_quantile,
            )
        )

    combined = torch.cat(parts, dim=0)  # [total_queries, 6]

    # Shuffle so density-biased and uniform queries are interleaved
    perm = torch.randperm(combined.shape[0])
    return combined[perm]


def generate_spatiotemporal_queries(
    trajectories: List[Tensor],
    n_queries: int = 100,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
    anchor_to_data: bool = True,
) -> Tensor:
    """Generate random spatiotemporal range queries from trajectory bounds."""
    if anchor_to_data:
        return generate_density_biased_queries(
            trajectories,
            n_queries=n_queries,
            spatial_fraction=spatial_fraction,
            temporal_fraction=temporal_fraction,
        )
    return generate_uniform_queries(
        trajectories,
        n_queries=n_queries,
        spatial_fraction=spatial_fraction,
        temporal_fraction=temporal_fraction,
    )


# ---------------------------------------------------------------------------
# Typed query generators (return list[dict] with "type" and "params")
# ---------------------------------------------------------------------------


def _tensor_queries_to_typed(
    queries: Tensor,
    query_type: str,
) -> list[dict[str, Any]]:
    """Convert a [M, 6] range-query tensor to a list of typed query dicts."""
    result: list[dict[str, Any]] = []
    for i in range(queries.shape[0]):
        q = queries[i]
        result.append(
            {
                "type": query_type,
                "params": {
                    "lat_min": float(q[0].item()),
                    "lat_max": float(q[1].item()),
                    "lon_min": float(q[2].item()),
                    "lon_max": float(q[3].item()),
                    "time_start": float(q[4].item()),
                    "time_end": float(q[5].item()),
                },
            }
        )
    return result


def generate_intersection_queries(
    trajectories: List[Tensor],
    n_queries: int = 100,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
) -> list[dict[str, Any]]:
    """Generate intersection queries anchored to high-density AIS regions."""
    tensor_queries = generate_density_biased_queries(
        trajectories,
        n_queries=n_queries,
        spatial_fraction=spatial_fraction,
        temporal_fraction=temporal_fraction,
    )
    return _tensor_queries_to_typed(tensor_queries, "intersection")


def generate_aggregation_queries(
    trajectories: List[Tensor],
    n_queries: int = 100,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
    spatial_bound_lower_quantile: float = 0.01,
    spatial_bound_upper_quantile: float = 0.99,
) -> list[dict[str, Any]]:
    """Generate aggregation queries with uniformly sampled centres."""
    tensor_queries = generate_uniform_queries(
        trajectories,
        n_queries=n_queries,
        spatial_fraction=spatial_fraction,
        temporal_fraction=temporal_fraction,
        spatial_bound_lower_quantile=spatial_bound_lower_quantile,
        spatial_bound_upper_quantile=spatial_bound_upper_quantile,
    )
    return _tensor_queries_to_typed(tensor_queries, "aggregation")


def generate_nearest_neighbor_queries(
    trajectories: List[Tensor],
    n_queries: int = 100,
    noise_lat: float = 0.05,
    noise_lon: float = 0.05,
    time_window_fraction: float = 0.05,
    k: int = 1,
) -> list[dict[str, Any]]:
    """Generate k-nearest-neighbor queries sampled from real AIS data points.

    Query points are anchored to real AIS positions with a small amount of
    spatial jitter, which ensures realistic coverage.  The ``k`` parameter
    controls how many nearest neighbours are considered per query; when
    ``k=1`` (the default) the result is the classical single-NN distance.

    Args:
        trajectories: List of per-ship point tensors [T, F].
        n_queries: Number of queries to generate.
        noise_lat: Standard deviation of Gaussian latitude noise (degrees).
        noise_lon: Standard deviation of Gaussian longitude noise (degrees).
        time_window_fraction: Time window half-width as a fraction of the
            overall time range.  Points within ``query_time ± time_window``
            are searched for the nearest neighbours.
        k: Number of nearest neighbours to return per query (default: 1).
            Must be >= 1.  If the available candidate pool is smaller than
            ``k``, all available points are used.

    Returns:
        List of typed query dicts with ``"type": "nearest"`` and a ``"k"``
        entry in ``"params"``.

    Raises:
        ValueError: If ``k < 1``.
    """
    if k < 1:
        raise ValueError(f"k must be >= 1, got {k}.")

    all_points = torch.cat(trajectories, dim=0)  # [N, F]
    n_points = all_points.shape[0]

    time_min = all_points[:, 0].min()
    time_max = all_points[:, 0].max()
    time_range = _guard_range(time_max - time_min)
    time_window = float((time_window_fraction * time_range).item())

    anchor_idx = torch.randint(0, n_points, (n_queries,))
    anchors = all_points[anchor_idx]

    query_lats = (anchors[:, 1] + torch.randn(n_queries) * noise_lat).tolist()
    query_lons = (anchors[:, 2] + torch.randn(n_queries) * noise_lon).tolist()
    query_times = anchors[:, 0].tolist()

    result: list[dict[str, Any]] = []
    for qlat, qlon, qtime in zip(query_lats, query_lons, query_times):
        result.append(
            {
                "type": "nearest",
                "params": {
                    "query_lat": float(qlat),
                    "query_lon": float(qlon),
                    "query_time": float(qtime),
                    "time_window": time_window,
                    "k": k,
                },
            }
        )
    return result


def generate_multi_type_workload(
    trajectories: List[Tensor],
    total_queries: int = 100,
    ratios: dict[str, float] | None = None,
    spatial_fraction: float = 0.03,
    temporal_fraction: float = 0.10,
    spatial_bound_lower_quantile: float = 0.01,
    spatial_bound_upper_quantile: float = 0.99,
    noise_lat: float = 0.05,
    noise_lon: float = 0.05,
    time_window_fraction: float = 0.05,
    k: int = 1,
) -> list[dict[str, Any]]:
    """Generate a mixed workload containing multiple query types.

    Args:
        trajectories: List of per-ship point tensors [T, F].
        total_queries: Total number of queries across all types.
        ratios: Dict mapping query type names to their fraction of the workload.
            Keys must be a subset of ``{"range", "intersection", "aggregation",
            "nearest"}``.  Values must be non-negative and sum to 1.0 (or
            close to it).  Defaults to equal split across all four types.
        spatial_fraction: Spatial width fraction for range/intersection/
            aggregation queries.
        temporal_fraction: Temporal width fraction for range/intersection/
            aggregation queries.
        spatial_bound_lower_quantile: Lower quantile for uniform query bounds.
        spatial_bound_upper_quantile: Upper quantile for uniform query bounds.
        noise_lat: Latitude noise for nearest-neighbor query anchors.
        noise_lon: Longitude noise for nearest-neighbor query anchors.
        time_window_fraction: Time-window fraction for nearest-neighbor queries.
        k: Number of nearest neighbours for nearest-type queries (default: 1).

    Returns:
        Shuffled list of typed query dicts.

    Raises:
        ValueError: If ratios contain unknown query types or do not form a
            valid probability distribution.
    """
    from src.queries.query_types import VALID_QUERY_TYPES

    if ratios is None:
        ratios = {"range": 0.25, "intersection": 0.25, "aggregation": 0.25, "nearest": 0.25}

    unknown = set(ratios) - VALID_QUERY_TYPES
    if unknown:
        raise ValueError(
            f"Unknown query type(s) in ratios: {unknown}. "
            f"Valid types: {sorted(VALID_QUERY_TYPES)}"
        )

    total_ratio = sum(ratios.values())
    if abs(total_ratio - 1.0) > 1e-6:
        raise ValueError(
            f"Ratios must sum to 1.0, got {total_ratio:.6f}. "
            "Normalise the values or adjust them to sum exactly to 1."
        )

    # Compute per-type counts using floored rounding; remainder goes to the
    # largest group to ensure total_queries is met exactly.
    counts: dict[str, int] = {}
    for qt, r in ratios.items():
        counts[qt] = int(total_queries * r)

    assigned = sum(counts.values())
    remainder = total_queries - assigned
    if remainder > 0:
        largest = max(counts, key=lambda query_type: ratios[query_type])
        counts[largest] += remainder

    parts: list[dict[str, Any]] = []

    n_range = counts.get("range", 0)
    if n_range > 0:
        rq = generate_density_biased_queries(
            trajectories,
            n_queries=n_range,
            spatial_fraction=spatial_fraction,
            temporal_fraction=temporal_fraction,
        )
        parts.extend(_tensor_queries_to_typed(rq, "range"))

    n_intersection = counts.get("intersection", 0)
    if n_intersection > 0:
        parts.extend(
            generate_intersection_queries(
                trajectories,
                n_queries=n_intersection,
                spatial_fraction=spatial_fraction,
                temporal_fraction=temporal_fraction,
            )
        )

    n_aggregation = counts.get("aggregation", 0)
    if n_aggregation > 0:
        parts.extend(
            generate_aggregation_queries(
                trajectories,
                n_queries=n_aggregation,
                spatial_fraction=spatial_fraction,
                temporal_fraction=temporal_fraction,
                spatial_bound_lower_quantile=spatial_bound_lower_quantile,
                spatial_bound_upper_quantile=spatial_bound_upper_quantile,
            )
        )

    n_nearest = counts.get("nearest", 0)
    if n_nearest > 0:
        parts.extend(
            generate_nearest_neighbor_queries(
                trajectories,
                n_queries=n_nearest,
                noise_lat=noise_lat,
                noise_lon=noise_lon,
                time_window_fraction=time_window_fraction,
                k=k,
            )
        )

    # Shuffle so query types are interleaved
    perm = torch.randperm(len(parts)).tolist()
    return [parts[i] for i in perm]
