"""Query workload generation for the four AIS-QDS v2 query types. See src/queries/README.md for details."""

from __future__ import annotations

from typing import Any

import torch

from src.experiments.experiment_config import TypedQueryWorkload
from src.queries.query_types import normalize_workload_mix, pad_query_features

DENSITY_ANCHOR_PROBABILITY = 0.70
DENSITY_GRID_BINS = 64
DEFAULT_RANGE_SPATIAL_FRACTION = 0.08
DEFAULT_RANGE_TIME_FRACTION = 0.15
DEFAULT_SIMILARITY_RADIUS_FRACTION = 0.04
DEFAULT_SIMILARITY_TIME_FRACTION = 0.04
DEFAULT_KNN_T_HALF_WINDOW_FRACTION = 0.25
DEFAULT_KNN_K = 12


def _dataset_bounds(points: torch.Tensor) -> dict[str, float]:
    """Compute global point-cloud bounds for query generation. See src/queries/README.md for details."""
    return {
        "t_min": float(points[:, 0].min().item()),
        "t_max": float(points[:, 0].max().item()),
        "lat_min": float(points[:, 1].min().item()),
        "lat_max": float(points[:, 1].max().item()),
        "lon_min": float(points[:, 2].min().item()),
        "lon_max": float(points[:, 2].max().item()),
    }


def _density_anchor_weights(points: torch.Tensor, bins: int = DENSITY_GRID_BINS) -> torch.Tensor:
    """Return per-point spatial density weights from a lat/lon grid density map."""
    if points.shape[0] == 0:
        return torch.empty((0,), dtype=torch.float32, device=points.device)

    bin_count = max(1, int(bins))
    lat = points[:, 1]
    lon = points[:, 2]
    lat_min = lat.min()
    lon_min = lon.min()
    lat_span = torch.clamp(lat.max() - lat_min, min=1e-6)
    lon_span = torch.clamp(lon.max() - lon_min, min=1e-6)

    lat_bins = torch.clamp(((lat - lat_min) / lat_span * (bin_count - 1)).long(), 0, bin_count - 1)
    lon_bins = torch.clamp(((lon - lon_min) / lon_span * (bin_count - 1)).long(), 0, bin_count - 1)
    bin_ids = lat_bins * bin_count + lon_bins
    cell_counts = torch.bincount(bin_ids.cpu(), minlength=bin_count * bin_count).to(
        device=points.device,
        dtype=torch.float32,
    )
    weights = cell_counts[bin_ids]
    total = weights.sum()
    if float(total.item()) <= 0.0:
        return torch.ones((points.shape[0],), dtype=torch.float32, device=points.device) / max(1, points.shape[0])
    return weights / total


def _weighted_sample_one(
    weights: torch.Tensor,
    generator: torch.Generator,
) -> int:
    """Sample one index from a non-negative weight vector.

    torch.multinomial caps at 2^24 categories, which the AIS combined-day CSVs
    exceed (23M+ points). Falls back to inverse-CDF sampling via cumsum +
    searchsorted, which has no size limit.
    """
    n = int(weights.numel())
    if n == 0:
        return 0
    total = float(weights.sum().item())
    if total <= 0.0:
        return int(torch.randint(0, n, (1,), generator=generator).item())
    if n <= (1 << 24):
        return int(torch.multinomial(weights, 1, generator=generator).item())
    cdf = torch.cumsum(weights, dim=0)
    r = float(torch.rand(1, generator=generator).item()) * total
    return int(torch.searchsorted(cdf, torch.tensor(r, dtype=cdf.dtype)).item())


def _pick_point(
    points: torch.Tensor,
    generator: torch.Generator,
    candidate_mask: torch.Tensor | None = None,
    density_weights: torch.Tensor | None = None,
    density_probability: float = DENSITY_ANCHOR_PROBABILITY,
) -> torch.Tensor:
    """Sample one point row from the cloud. See src/queries/README.md for details."""
    if candidate_mask is not None and bool(candidate_mask.any().item()):
        candidates = torch.where(candidate_mask)[0]
    else:
        candidates = None

    use_density = (
        density_weights is not None
        and density_weights.numel() == points.shape[0]
        and float(torch.rand(1, generator=generator).item()) < float(density_probability)
    )
    if use_density:
        if candidates is not None:
            candidate_weights = density_weights[candidates].float()
            if float(candidate_weights.sum().item()) > 0.0:
                sampled = _weighted_sample_one(candidate_weights, generator)
                return points[int(candidates[sampled].item())]
        else:
            weights = density_weights.float()
            if float(weights.sum().item()) > 0.0:
                sampled = _weighted_sample_one(weights, generator)
                return points[sampled]

    if candidates is not None:
        candidate_idx = int(torch.randint(0, candidates.shape[0], (1,), generator=generator).item())
        idx = int(candidates[candidate_idx].item())
    else:
        idx = int(torch.randint(0, points.shape[0], (1,), generator=generator).item())
    return points[idx]


def _make_range_query(
    points: torch.Tensor,
    b: dict[str, float],
    generator: torch.Generator,
    anchor_mask: torch.Tensor | None = None,
    density_weights: torch.Tensor | None = None,
    range_spatial_fraction: float = DEFAULT_RANGE_SPATIAL_FRACTION,
    range_time_fraction: float = DEFAULT_RANGE_TIME_FRACTION,
) -> dict[str, Any]:
    """Generate one range query. See src/queries/README.md for details."""
    spatial_fraction = float(range_spatial_fraction)
    time_fraction = float(range_time_fraction)
    if spatial_fraction <= 0.0 or time_fraction <= 0.0:
        raise ValueError("range_spatial_fraction and range_time_fraction must be positive.")
    p = _pick_point(points, generator, candidate_mask=anchor_mask, density_weights=density_weights)
    lat_w = spatial_fraction * (b["lat_max"] - b["lat_min"]) * (0.5 + torch.rand(1, generator=generator).item())
    lon_w = spatial_fraction * (b["lon_max"] - b["lon_min"]) * (0.5 + torch.rand(1, generator=generator).item())
    t_w = time_fraction * (b["t_max"] - b["t_min"]) * (0.5 + torch.rand(1, generator=generator).item())
    return {
        "type": "range",
        "params": {
            "lat_min": float(max(b["lat_min"], p[1].item() - lat_w)),
            "lat_max": float(min(b["lat_max"], p[1].item() + lat_w)),
            "lon_min": float(max(b["lon_min"], p[2].item() - lon_w)),
            "lon_max": float(min(b["lon_max"], p[2].item() + lon_w)),
            "t_start": float(max(b["t_min"], p[0].item() - t_w)),
            "t_end": float(min(b["t_max"], p[0].item() + t_w)),
        },
    }


def _make_knn_query(
    points: torch.Tensor,
    b: dict[str, float],
    generator: torch.Generator,
    anchor_mask: torch.Tensor | None = None,
    density_weights: torch.Tensor | None = None,
    knn_k: int | None = DEFAULT_KNN_K,
    knn_t_half_window_fraction: float = DEFAULT_KNN_T_HALF_WINDOW_FRACTION,
) -> dict[str, Any]:
    """Generate one kNN query. See src/queries/README.md for details."""
    p = _pick_point(points, generator, candidate_mask=anchor_mask, density_weights=density_weights)
    k = int(knn_k) if knn_k is not None and int(knn_k) > 0 else int(torch.randint(3, 8, (1,), generator=generator).item())
    return {
        "type": "knn",
        "params": {
            "lat": float(p[1].item()),
            "lon": float(p[2].item()),
            "t_center": float(p[0].item()),
            "t_half_window": float(knn_t_half_window_fraction * (b["t_max"] - b["t_min"])),
            "k": max(1, k),
        },
    }


def _make_similarity_query(
    points: torch.Tensor,
    trajectories: list[torch.Tensor],
    b: dict[str, float],
    generator: torch.Generator,
    anchor_mask: torch.Tensor | None = None,
    similarity_time_fraction: float = DEFAULT_SIMILARITY_TIME_FRACTION,
) -> dict[str, Any]:
    """Generate one similarity query with a reference snippet. See src/queries/README.md for details."""
    p = _pick_point(points, generator, candidate_mask=anchor_mask)
    t_half = similarity_time_fraction * (b["t_max"] - b["t_min"])
    radius = DEFAULT_SIMILARITY_RADIUS_FRACTION * max(b["lat_max"] - b["lat_min"], b["lon_max"] - b["lon_min"])

    traj_idx = int(torch.randint(0, len(trajectories), (1,), generator=generator).item())
    traj = trajectories[traj_idx]
    center = int(torch.randint(2, max(3, traj.shape[0] - 2), (1,), generator=generator).item())
    ref = traj[max(0, center - 2) : min(traj.shape[0], center + 3), :3]

    return {
        "type": "similarity",
        "params": {
            "lat_query_centroid": float(p[1].item()),
            "lon_query_centroid": float(p[2].item()),
            "t_start": float(max(b["t_min"], p[0].item() - t_half)),
            "t_end": float(min(b["t_max"], p[0].item() + t_half)),
            "radius": float(radius),
            "top_k": 5,
        },
        "reference": ref.tolist(),
    }


def _make_clustering_query(
    points: torch.Tensor,
    b: dict[str, float],
    generator: torch.Generator,
    anchor_mask: torch.Tensor | None = None,
    density_weights: torch.Tensor | None = None,
    range_spatial_fraction: float = DEFAULT_RANGE_SPATIAL_FRACTION,
    range_time_fraction: float = DEFAULT_RANGE_TIME_FRACTION,
) -> dict[str, Any]:
    """Generate one clustering query. See src/queries/README.md for details."""
    rq = _make_range_query(
        points,
        b,
        generator,
        anchor_mask=anchor_mask,
        density_weights=density_weights,
        range_spatial_fraction=range_spatial_fraction,
        range_time_fraction=range_time_fraction,
    )
    params = dict(rq["params"])
    params.update(
        {
            "eps": float(0.02 * max(b["lat_max"] - b["lat_min"], b["lon_max"] - b["lon_min"])),
            "min_samples": int(torch.randint(3, 7, (1,), generator=generator).item()),
        }
    )
    return {"type": "clustering", "params": params}


def _box_mask(points: torch.Tensor, params: dict[str, float]) -> torch.Tensor:
    """Return the point mask inside a spatiotemporal query box."""
    return (
        (points[:, 1] >= params["lat_min"])
        & (points[:, 1] <= params["lat_max"])
        & (points[:, 2] >= params["lon_min"])
        & (points[:, 2] <= params["lon_max"])
        & (points[:, 0] >= params["t_start"])
        & (points[:, 0] <= params["t_end"])
    )


def _haversine_km(lat1: torch.Tensor, lon1: torch.Tensor, lat2: float, lon2: float) -> torch.Tensor:
    """Compute haversine distances in km to one anchor point."""
    import math

    r = 6371.0
    lat1r = torch.deg2rad(lat1)
    lon1r = torch.deg2rad(lon1)
    lat2r = math.radians(lat2)
    lon2r = math.radians(lon2)
    dlat = lat1r - lat2r
    dlon = lon1r - lon2r
    a = torch.sin(dlat / 2.0) ** 2 + torch.cos(lat1r) * math.cos(lat2r) * torch.sin(dlon / 2.0) ** 2
    c = 2.0 * torch.atan2(torch.sqrt(a), torch.sqrt(torch.clamp(1.0 - a, min=1e-9)))
    return r * c


def point_coverage_mask_for_query(points: torch.Tensor, query: dict[str, Any]) -> torch.Tensor:
    """Return the point-level dataset coverage induced by one query.

    Coverage follows the same regions that produce query-specific training
    signal: exact boxes for range/clustering, a dense neighbourhood around the
    kNN anchor within the time window, and the spatiotemporal radius for
    similarity queries.
    """
    mask = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    if points.numel() == 0:
        return mask

    qtype = str(query["type"]).lower()
    params = query["params"]
    if qtype in {"range", "clustering"}:
        return _box_mask(points, params)

    if qtype == "knn":
        t0 = float(params["t_center"] - params["t_half_window"])
        t1 = float(params["t_center"] + params["t_half_window"])
        in_window = (points[:, 0] >= t0) & (points[:, 0] <= t1)
        cand = torch.where(in_window)[0]
        if cand.numel() == 0:
            return mask
        cand_points = points[cand]
        d_space = _haversine_km(cand_points[:, 1], cand_points[:, 2], float(params["lat"]), float(params["lon"]))
        d_time = torch.abs(cand_points[:, 0] - float(params["t_center"]))
        dist = d_space + 0.001 * d_time
        k_eff = min(max(1, int(params["k"])), dist.numel())
        kth = torch.topk(-dist, k_eff).values[-1].neg()
        covered = cand[dist <= (3.0 * (float(kth.item()) + 1e-6))]
        mask[covered] = True
        return mask

    if qtype == "similarity":
        radius = float(params["radius"])
        return (
            (points[:, 0] >= params["t_start"])
            & (points[:, 0] <= params["t_end"])
            & (
                torch.sqrt(
                    (points[:, 1] - params["lat_query_centroid"]) ** 2
                    + (points[:, 2] - params["lon_query_centroid"]) ** 2
                )
                <= radius
            )
        )

    raise ValueError(f"Unsupported query type for coverage: {qtype}")


def query_coverage_mask(points: torch.Tensor, typed_queries: list[dict[str, Any]]) -> torch.Tensor:
    """Return the union of covered points for a workload."""
    covered = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    for query in typed_queries:
        covered |= point_coverage_mask_for_query(points, query)
    return covered


def query_coverage_fraction(points: torch.Tensor, typed_queries: list[dict[str, Any]]) -> float:
    """Return the fraction of points covered by a workload."""
    if points.shape[0] == 0:
        return 0.0
    return float(query_coverage_mask(points, typed_queries).float().mean().item())


def _normalize_target_coverage(target_coverage: float | None) -> float | None:
    """Normalize coverage targets supplied as fractions or percentages."""
    if target_coverage is None:
        return None
    target = float(target_coverage)
    if target > 1.0:
        if target <= 100.0:
            target = target / 100.0
        else:
            raise ValueError("target_coverage must be a fraction in (0, 1] or a percent in (0, 100].")
    if target <= 0.0 or target > 1.0:
        raise ValueError("target_coverage must be a fraction in (0, 1] or a percent in (0, 100].")
    return target


def _make_query(
    name: str,
    points: torch.Tensor,
    trajectories: list[torch.Tensor],
    b: dict[str, float],
    generator: torch.Generator,
    anchor_mask: torch.Tensor | None = None,
    density_weights: torch.Tensor | None = None,
    range_spatial_fraction: float = DEFAULT_RANGE_SPATIAL_FRACTION,
    range_time_fraction: float = DEFAULT_RANGE_TIME_FRACTION,
    knn_k: int | None = DEFAULT_KNN_K,
    knn_t_half_window_fraction: float = DEFAULT_KNN_T_HALF_WINDOW_FRACTION,
    similarity_time_fraction: float = DEFAULT_SIMILARITY_TIME_FRACTION,
) -> dict[str, Any]:
    """Generate one query of a named type."""
    if name == "range":
        return _make_range_query(
            points,
            b,
            generator,
            anchor_mask=anchor_mask,
            density_weights=density_weights,
            range_spatial_fraction=range_spatial_fraction,
            range_time_fraction=range_time_fraction,
        )
    if name == "knn":
        return _make_knn_query(
            points,
            b,
            generator,
            anchor_mask=anchor_mask,
            density_weights=density_weights,
            knn_k=knn_k,
            knn_t_half_window_fraction=knn_t_half_window_fraction,
        )
    if name == "similarity":
        return _make_similarity_query(
            points,
            trajectories,
            b,
            generator,
            anchor_mask=anchor_mask,
            similarity_time_fraction=similarity_time_fraction,
        )
    if name == "clustering":
        return _make_clustering_query(
            points,
            b,
            generator,
            anchor_mask=anchor_mask,
            density_weights=density_weights,
            range_spatial_fraction=range_spatial_fraction,
            range_time_fraction=range_time_fraction,
        )
    raise ValueError(f"Unsupported query type: {name}")


def _finalize_workload(
    points: torch.Tensor,
    typed: list[dict[str, Any]],
    generator: torch.Generator,
) -> TypedQueryWorkload:
    """Shuffle, featurize, and attach point-coverage metadata."""
    if typed:
        perm = torch.randperm(len(typed), generator=generator).tolist()
        typed = [typed[i] for i in perm]

    features, type_ids = pad_query_features(typed)
    covered = query_coverage_mask(points, typed)
    covered_points = int(covered.sum().item())
    total_points = int(points.shape[0])
    coverage_fraction = float(covered_points / total_points) if total_points > 0 else 0.0
    return TypedQueryWorkload(
        query_features=features,
        typed_queries=typed,
        type_ids=type_ids,
        coverage_fraction=coverage_fraction,
        covered_points=covered_points,
        total_points=total_points,
    )


def generate_typed_query_workload(
    trajectories: list[torch.Tensor],
    n_queries: int,
    workload_mix: dict[str, float],
    seed: int,
    target_coverage: float | None = None,
    max_queries: int | None = None,
    range_spatial_fraction: float = DEFAULT_RANGE_SPATIAL_FRACTION,
    range_time_fraction: float = DEFAULT_RANGE_TIME_FRACTION,
    knn_k: int | None = DEFAULT_KNN_K,
    knn_t_half_window_fraction: float = DEFAULT_KNN_T_HALF_WINDOW_FRACTION,
    similarity_time_fraction: float = DEFAULT_SIMILARITY_TIME_FRACTION,
    front_load_knn: int = 0,
) -> TypedQueryWorkload:
    """Generate a mixed typed-query workload and padded feature tensor. See src/queries/README.md for details.

    front_load_knn: generate this many kNN queries first, before proportional
    scheduling starts.  Use this when kNN weight is high but coverage is low,
    so kNN queries are guaranteed their full quota even if generation stops early.
    """
    points = torch.cat(trajectories, dim=0)
    b = _dataset_bounds(points)

    mix = normalize_workload_mix(workload_mix)
    g = torch.Generator().manual_seed(int(seed))
    density_weights = _density_anchor_weights(points)

    names = list(mix.keys())
    weights = torch.tensor([mix[n] for n in names], dtype=torch.float32)
    coverage_target = _normalize_target_coverage(target_coverage)

    if coverage_target is not None:
        requested_queries = max(1, int(n_queries))
        if max_queries is not None and int(max_queries) <= 0:
            raise ValueError("max_queries must be positive when target_coverage is set.")
        typed: list[dict[str, Any]] = []
        counts = torch.zeros((len(names),), dtype=torch.long)
        covered = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)

        query_limit = max(requested_queries, int(max_queries) if max_queries is not None else requested_queries)

        # Generate kNN queries first so they always get their initial quota even
        # when other types advance coverage faster.
        if front_load_knn > 0 and "knn" in names:
            knn_idx = names.index("knn")
            for _ in range(min(front_load_knn, query_limit)):
                query = _make_query(
                    "knn", points, trajectories, b, g,
                    anchor_mask=None,
                    density_weights=density_weights,
                    range_spatial_fraction=range_spatial_fraction,
                    range_time_fraction=range_time_fraction,
                    knn_k=knn_k,
                    knn_t_half_window_fraction=knn_t_half_window_fraction,
                    similarity_time_fraction=similarity_time_fraction,
                )
                typed.append(query)
                counts[knn_idx] += 1
                covered |= point_coverage_mask_for_query(points, query)

        while len(typed) < query_limit:
            current_coverage = float(covered.float().mean().item()) if points.shape[0] > 0 else 0.0
            if len(typed) >= requested_queries and current_coverage >= coverage_target:
                break
            desired = weights * float(len(typed) + 1)
            type_idx = int(torch.argmax(desired - counts.float()).item())
            name = names[type_idx]
            anchor_mask = (~covered) if current_coverage < coverage_target else None
            query = _make_query(
                name,
                points,
                trajectories,
                b,
                g,
                anchor_mask=anchor_mask,
                density_weights=density_weights,
                range_spatial_fraction=range_spatial_fraction,
                range_time_fraction=range_time_fraction,
                knn_k=knn_k,
            )
            typed.append(query)
            counts[type_idx] += 1
            covered |= point_coverage_mask_for_query(points, query)

        return _finalize_workload(points, typed, g)

    counts = torch.floor(weights * n_queries).to(torch.long)
    while int(counts.sum().item()) < n_queries:
        idx = int(torch.argmax(weights - counts.float() / max(1, n_queries)).item())
        counts[idx] += 1

    typed: list[dict[str, Any]] = []
    # Front-load kNN queries before proportional types
    if front_load_knn > 0 and "knn" in names:
        knn_idx = names.index("knn")
        n_front = min(front_load_knn, int(counts[knn_idx].item()))
        for _ in range(n_front):
            typed.append(_make_query(
                "knn", points, trajectories, b, g,
                density_weights=density_weights,
                range_spatial_fraction=range_spatial_fraction,
                range_time_fraction=range_time_fraction,
                knn_k=knn_k,
                knn_t_half_window_fraction=knn_t_half_window_fraction,
                similarity_time_fraction=similarity_time_fraction,
            ))
        counts[knn_idx] = max(0, counts[knn_idx] - n_front)
    for name, count in zip(names, counts.tolist()):
        for _ in range(int(count)):
            typed.append(
                _make_query(
                    name,
                    points,
                    trajectories,
                    b,
                    g,
                    density_weights=density_weights,
                    range_spatial_fraction=range_spatial_fraction,
                    range_time_fraction=range_time_fraction,
                    knn_k=knn_k,
                    knn_t_half_window_fraction=knn_t_half_window_fraction,
                    similarity_time_fraction=similarity_time_fraction,
                )
            )

    return _finalize_workload(points, typed, g)
