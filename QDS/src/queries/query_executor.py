"""Typed query execution against flattened or trajectory AIS data. See src/queries/README.md for details."""

from __future__ import annotations

import math

import torch


def _boundaries_from_trajectories(trajectories: list[torch.Tensor]) -> list[tuple[int, int]]:
    """Build flattened point boundaries from trajectory order."""
    boundaries: list[tuple[int, int]] = []
    cursor = 0
    for traj in trajectories:
        n = int(traj.shape[0])
        boundaries.append((cursor, cursor + n))
        cursor += n
    return boundaries


def _default_boundaries(points: torch.Tensor, boundaries: list[tuple[int, int]] | None) -> list[tuple[int, int]]:
    """Use explicit boundaries when supplied, otherwise treat all points as one trajectory."""
    return boundaries if boundaries is not None else [(0, int(points.shape[0]))]


def _indices_to_trajectory_ids(indices: torch.Tensor, boundaries: list[tuple[int, int]]) -> set[int]:
    """Map flattened point indices to stable trajectory IDs derived from boundaries."""
    if indices.numel() == 0:
        return set()
    trajectory_ids: set[int] = set()
    for traj_id, (start, end) in enumerate(boundaries):
        if end <= start:
            continue
        if bool(((indices >= start) & (indices < end)).any().item()):
            trajectory_ids.add(traj_id)
    return trajectory_ids


def _haversine_km(lat1: torch.Tensor, lon1: torch.Tensor, lat2: float, lon2: float) -> torch.Tensor:
    """Compute haversine distances in km to one anchor point. See src/queries/README.md for details."""
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


def _box_mask(points: torch.Tensor, params: dict[str, float]) -> torch.Tensor:
    """Return the point mask inside a spatiotemporal query box."""
    mask = (
        (points[:, 1] >= params["lat_min"])
        & (points[:, 1] <= params["lat_max"])
        & (points[:, 2] >= params["lon_min"])
        & (points[:, 2] <= params["lon_max"])
        & (points[:, 0] >= params["t_start"])
        & (points[:, 0] <= params["t_end"])
    )
    return mask


def execute_range_query(
    points: torch.Tensor,
    params: dict[str, float],
    boundaries: list[tuple[int, int]] | None = None,
) -> set[int]:
    """Execute a range query returning matching trajectory IDs. See src/queries/README.md for details."""
    mask = _box_mask(points, params)
    if not mask.any():
        return set()
    return _indices_to_trajectory_ids(torch.where(mask)[0], _default_boundaries(points, boundaries))


def execute_knn_query(
    points: torch.Tensor,
    params: dict[str, float],
    boundaries: list[tuple[int, int]] | None = None,
) -> set[int]:
    """Execute a kNN query returning the nearest distinct trajectory IDs."""
    k = max(1, int(params["k"]))
    t0 = float(params["t_center"] - params["t_half_window"])
    t1 = float(params["t_center"] + params["t_half_window"])
    mask = (points[:, 0] >= t0) & (points[:, 0] <= t1)
    idx = torch.where(mask)[0]
    if idx.numel() == 0:
        return set()

    cand = points[idx]
    d_space = _haversine_km(cand[:, 1], cand[:, 2], float(params["lat"]), float(params["lon"]))
    d_time = torch.abs(cand[:, 0] - float(params["t_center"]))
    dist = d_space + 0.001 * d_time

    query_boundaries = _default_boundaries(points, boundaries)
    ends = torch.tensor([end for _, end in query_boundaries], dtype=torch.long, device=idx.device)
    traj_ids = torch.bucketize(idx, ends, right=True)
    min_dist = torch.full((len(query_boundaries),), float("inf"), dtype=dist.dtype, device=dist.device)
    min_dist.scatter_reduce_(0, traj_ids, dist, reduce="amin", include_self=True)
    valid = torch.isfinite(min_dist)
    if not bool(valid.any().item()):
        return set()

    valid_ids = torch.where(valid)[0]
    k_eff = min(k, int(valid_ids.numel()))
    chosen = torch.topk(-min_dist[valid_ids], k_eff).indices
    return {int(trajectory_id) for trajectory_id in valid_ids[chosen].tolist()}


def _dtw_like_distance(a: torch.Tensor, b: torch.Tensor) -> float:
    """Compute a lightweight DTW-like distance for short snippets. See src/queries/README.md for details."""
    if a.numel() == 0 or b.numel() == 0:
        return float("inf")
    la = a.shape[0]
    lb = b.shape[0]
    n = max(la, lb)
    ia = torch.linspace(0, la - 1, n).long()
    ib = torch.linspace(0, lb - 1, n).long()
    pa = a[ia]
    pb = b[ib]
    d = torch.norm(pa[:, 1:3] - pb[:, 1:3], dim=1)
    return float(d.mean().item())


def execute_similarity_query(
    trajectories: list[torch.Tensor],
    params: dict[str, float],
    reference: list[list[float]],
) -> list[int]:
    """Execute a similarity query returning ranked trajectory indices. See src/queries/README.md for details."""
    ref = torch.tensor(reference, dtype=torch.float32)
    ranked: list[tuple[int, float]] = []

    for i, traj in enumerate(trajectories):
        mask_t = (traj[:, 0] >= params["t_start"]) & (traj[:, 0] <= params["t_end"])
        if not mask_t.any():
            continue
        segment = traj[mask_t][:, :3]
        centroid = segment[:, 1:3].mean(dim=0)
        if torch.norm(centroid - torch.tensor([params["lat_query_centroid"], params["lon_query_centroid"]])) > params["radius"]:
            continue
        d = _dtw_like_distance(segment, ref)
        ranked.append((i, d))

    ranked.sort(key=lambda x: x[1])
    top_k = int(params.get("top_k", 5))
    return [idx for idx, _ in ranked[:top_k]]


def _dbscan_labels(points_xy: torch.Tensor, eps: float, min_samples: int) -> list[int]:
    """Compute simple DBSCAN labels for representative points."""
    if points_xy.shape[0] == 0:
        return []
    n = points_xy.shape[0]
    visited = torch.zeros(n, dtype=torch.bool)
    labels = torch.full((n,), -1, dtype=torch.long)
    cluster_id = 0

    dmat = torch.cdist(points_xy, points_xy)

    for i in range(n):
        if visited[i]:
            continue
        visited[i] = True
        neigh = torch.where(dmat[i] <= eps)[0]
        if neigh.numel() < min_samples:
            labels[i] = -1
            continue
        labels[i] = cluster_id
        seeds = neigh.tolist()
        ptr = 0
        while ptr < len(seeds):
            j = seeds[ptr]
            ptr += 1
            if not visited[j]:
                visited[j] = True
                neigh_j = torch.where(dmat[j] <= eps)[0]
                if neigh_j.numel() >= min_samples:
                    seeds.extend(neigh_j.tolist())
            if labels[j] == -1:
                labels[j] = cluster_id
        cluster_id += 1

    return [int(v) for v in labels.tolist()]


def execute_clustering_query(
    points: torch.Tensor,
    params: dict[str, float],
    boundaries: list[tuple[int, int]] | None = None,
) -> list[int]:
    """Execute clustering over trajectory centroids and return per-trajectory labels."""
    query_boundaries = _default_boundaries(points, boundaries)
    labels = [-1 for _ in query_boundaries]
    representatives: list[torch.Tensor] = []
    represented_ids: list[int] = []

    for traj_id, (start, end) in enumerate(query_boundaries):
        if end <= start:
            continue
        traj_points = points[start:end]
        mask = _box_mask(traj_points, params)
        if not bool(mask.any().item()):
            continue
        representatives.append(traj_points[mask, 1:3].mean(dim=0))
        represented_ids.append(traj_id)

    if not representatives:
        return labels

    rep_xy = torch.stack(representatives)
    rep_labels = _dbscan_labels(rep_xy, eps=float(params["eps"]), min_samples=int(params["min_samples"]))
    for traj_id, label in zip(represented_ids, rep_labels):
        labels[traj_id] = int(label)
    return labels


def execute_typed_query(
    points: torch.Tensor,
    trajectories: list[torch.Tensor],
    query: dict,
    boundaries: list[tuple[int, int]] | None = None,
) -> set[int] | list[int]:
    """Execute one typed query and return type-specific result object. See src/queries/README.md for details."""
    qtype = query["type"]
    params = query["params"]
    query_boundaries = boundaries if boundaries is not None else _boundaries_from_trajectories(trajectories)
    if qtype == "range":
        return execute_range_query(points, params, query_boundaries)
    if qtype == "knn":
        return execute_knn_query(points, params, query_boundaries)
    if qtype == "similarity":
        return execute_similarity_query(trajectories, params, query.get("reference", []))
    if qtype == "clustering":
        return execute_clustering_query(points, params, query_boundaries)
    raise ValueError(f"Unsupported query type: {qtype}")
