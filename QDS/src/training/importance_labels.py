"""Typed F1-contribution label construction. See src/training/README.md for details."""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

import torch

from src.queries.query_executor import execute_typed_query
from src.queries.query_types import QUERY_NAME_TO_ID, NUM_QUERY_TYPES

KNN_REPRESENTATIVES_PER_TRAJECTORY = 64
SIMILARITY_REPRESENTATIVES_PER_TRAJECTORY = 64


def _box_mask(points: torch.Tensor, params: dict[str, float]) -> torch.Tensor:
    """Build spatiotemporal box mask. See src/training/README.md for details."""
    return (
        (points[:, 1] >= params["lat_min"])
        & (points[:, 1] <= params["lat_max"])
        & (points[:, 2] >= params["lon_min"])
        & (points[:, 2] <= params["lon_max"])
        & (points[:, 0] >= params["t_start"])
        & (points[:, 0] <= params["t_end"])
    )


def _trajectory_id_per_point(n_points: int, boundaries: list[tuple[int, int]], device: torch.device) -> torch.Tensor:
    """Build a point-index to trajectory-ID lookup."""
    trajectory_ids = torch.full((n_points,), -1, dtype=torch.long, device=device)
    for trajectory_id, (start, end) in enumerate(boundaries):
        if end > start:
            trajectory_ids[start:end] = int(trajectory_id)
    return trajectory_ids


def _trajectories_from_boundaries(points: torch.Tensor, boundaries: list[tuple[int, int]]) -> list[torch.Tensor]:
    """Slice flattened points into trajectories for query execution."""
    return [points[start:end] for start, end in boundaries]


def _ids_mask(point_trajectory_ids: torch.Tensor, trajectory_ids: set[int]) -> torch.Tensor:
    """Return points whose trajectory ID belongs to the supplied set."""
    mask = torch.zeros_like(point_trajectory_ids, dtype=torch.bool)
    for trajectory_id in trajectory_ids:
        mask |= point_trajectory_ids == int(trajectory_id)
    return mask


def _set_query_singleton_gain(original_ids: set[int]) -> float:
    """F1 gained when one true-positive trajectory ID is recovered from an empty answer."""
    if not original_ids:
        return 0.0
    return float(2.0 / (len(original_ids) + 1.0))


def _haversine_km(lat1: torch.Tensor, lon1: torch.Tensor, lat2: float, lon2: float) -> torch.Tensor:
    """Compute haversine distance in km to one anchor point."""
    radius_km = 6371.0
    lat1_rad = torch.deg2rad(lat1)
    lon1_rad = torch.deg2rad(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    delta_lat = lat1_rad - lat2_rad
    delta_lon = lon1_rad - lon2_rad
    haversine = (
        torch.sin(delta_lat / 2.0) ** 2
        + torch.cos(lat1_rad) * math.cos(lat2_rad) * torch.sin(delta_lon / 2.0) ** 2
    )
    central_angle = 2.0 * torch.atan2(torch.sqrt(haversine), torch.sqrt(torch.clamp(1.0 - haversine, min=1e-9)))
    return radius_km * central_angle


def _add_distributed_hit_label(labels: torch.Tensor, support: torch.Tensor, type_idx: int, gain: float) -> None:
    """Distribute one trajectory-hit gain over its interchangeable support points."""
    support_count = int(support.sum().item())
    if support_count <= 0:
        return
    labels[support, type_idx] += float(gain) / float(support_count)


def _add_weighted_hit_label(
    labels: torch.Tensor,
    support: torch.Tensor,
    type_idx: int,
    gain: float,
    weights: torch.Tensor,
) -> None:
    """Distribute a trajectory-hit gain over support points, scaled by per-point weights.

    weights[support] should be non-negative. Total mass added equals gain. Falls back to
    uniform distribution if all weights are zero.
    """
    support_idx = torch.where(support)[0]
    if support_idx.numel() == 0:
        return
    w = weights[support_idx]
    total = float(w.sum().item())
    if total <= 0.0:
        labels[support, type_idx] += float(gain) / float(support_idx.numel())
        return
    labels[support_idx, type_idx] += float(gain) * (w / total)


def _within_box_centroid_weights(
    points: torch.Tensor,
    box_mask: torch.Tensor,
    point_trajectory_ids: torch.Tensor,
) -> torch.Tensor:
    """Per-point weights for clustering: squared distance from each in-box point to its
    trajectory's in-box centroid (lat/lon).

    Squared distance amplifies the contrast between extremes (load-bearing for centroid
    stability) and near-centroid points (replaceable). Weights are normalized to mean=1
    per trajectory so total per-query label mass is preserved.
    """
    weights = torch.zeros(points.shape[0], dtype=torch.float32, device=points.device)
    in_box_idx = torch.where(box_mask)[0]
    if in_box_idx.numel() == 0:
        return weights

    box_traj_ids = point_trajectory_ids[in_box_idx]
    box_coords = points[in_box_idx, 1:3]
    for tid in torch.unique(box_traj_ids).tolist():
        local = torch.where(box_traj_ids == tid)[0]
        if local.numel() == 0:
            continue
        if local.numel() == 1:
            weights[in_box_idx[local]] = 1.0
            continue
        coords = box_coords[local]
        centroid = coords.mean(dim=0)
        dists_sq = torch.sum((coords - centroid) ** 2, dim=1)
        mean_d_sq = float(dists_sq.mean().item())
        if mean_d_sq > 1e-12:
            weights[in_box_idx[local]] = dists_sq / mean_d_sq
        else:
            weights[in_box_idx[local]] = 1.0
    return weights


def _range_boundary_weights(
    points: torch.Tensor,
    box_mask: torch.Tensor,
    point_trajectory_ids: torch.Tensor,
) -> torch.Tensor:
    """Per-point weights for range: boundary-only prior.

    Replaces the previous boundary+proximity combination. The proximity factor
    (1/distance to nearest in-box point of a different trajectory) required an
    O(N^2) cdist per query plus a Python loop over trajectory IDs with .item()
    syncs — significant setup-time cost on range/clustering workloads.

    Boundary crossings: in-box points whose immediate temporal neighbor is OUT
    of the box. These mark vessel entry/exit and are semantically critical for
    range queries. Boost: 2x. Weights are mean=1-normalized within each
    trajectory's in-box subset so total per-query label mass is preserved.
    """
    weights = torch.zeros(points.shape[0], dtype=torch.float32, device=points.device)
    in_box_idx = torch.where(box_mask)[0]
    n_box = in_box_idx.numel()
    if n_box == 0:
        return weights
    if n_box == 1:
        weights[in_box_idx] = 1.0
        return weights

    box_traj_ids = point_trajectory_ids[in_box_idx]

    # 1. Boundary detection: walk the FULL points tensor per trajectory, mark in-box
    # points whose neighbor is out-of-box (or that are at trajectory edges while in-box).
    boundary_full = torch.zeros(points.shape[0], dtype=torch.bool, device=points.device)
    for tid in torch.unique(box_traj_ids).tolist():
        traj_idx = torch.where(point_trajectory_ids == int(tid))[0]
        if traj_idx.numel() == 0:
            continue
        traj_in = box_mask[traj_idx]
        prev_out = torch.zeros_like(traj_in)
        prev_out[1:] = traj_in[1:] & ~traj_in[:-1]
        prev_out[0] = traj_in[0]
        next_out = torch.zeros_like(traj_in)
        next_out[:-1] = traj_in[:-1] & ~traj_in[1:]
        next_out[-1] = traj_in[-1]
        boundary_full[traj_idx] = prev_out | next_out

    # Boundary-only weights, normalized per-trajectory to preserve total mass.
    boundary_in_box = boundary_full[in_box_idx].float()
    BOUNDARY_BOOST = 1.0  # additive on top of base 1.0 → boundary points get 2x raw

    # Per-trajectory mean=1 normalization in one vectorized pass (no Python loop
    # and no .item() syncs vs the previous per-tid loop).
    raw = 1.0 + BOUNDARY_BOOST * boundary_in_box
    # Compute per-trajectory mean of raw, then divide.
    unique_tids, inverse = torch.unique(box_traj_ids, return_inverse=True)
    sums = torch.zeros(unique_tids.numel(), dtype=raw.dtype, device=raw.device)
    counts = torch.zeros(unique_tids.numel(), dtype=raw.dtype, device=raw.device)
    sums.scatter_add_(0, inverse, raw)
    counts.scatter_add_(0, inverse, torch.ones_like(raw))
    means = sums / counts.clamp(min=1.0)
    safe_means = torch.where(means > 1e-12, means, torch.ones_like(means))
    weights[in_box_idx] = raw / safe_means[inverse]
    return weights


def _within_box_uniqueness_weights(
    points: torch.Tensor,
    box_mask: torch.Tensor,
    point_trajectory_ids: torch.Tensor,
) -> torch.Tensor:
    """Legacy per-point weights for range — distance to nearest in-box point of a DIFFERENT
    trajectory. Reverted from the range branch in phase 5 (Oracle dropped −9% with this).
    Kept for reference / experimentation; not currently called.
    """
    weights = torch.zeros(points.shape[0], dtype=torch.float32, device=points.device)
    in_box_idx = torch.where(box_mask)[0]
    n_box = in_box_idx.numel()
    if n_box == 0:
        return weights
    if n_box == 1:
        weights[in_box_idx] = 1.0
        return weights

    coords = points[in_box_idx, 1:3]
    box_traj_ids = point_trajectory_ids[in_box_idx]
    dists = torch.cdist(coords, coords)
    same_traj = box_traj_ids.unsqueeze(0) == box_traj_ids.unsqueeze(1)
    dists = dists.masked_fill(same_traj, float("inf"))
    nearest_other = dists.min(dim=1).values
    finite = torch.isfinite(nearest_other)
    if not bool(finite.any().item()):
        weights[in_box_idx] = 1.0
        return weights

    for tid in torch.unique(box_traj_ids).tolist():
        local = torch.where(box_traj_ids == tid)[0]
        if local.numel() == 0:
            continue
        traj_dists = nearest_other[local]
        traj_finite = torch.isfinite(traj_dists)
        if not bool(traj_finite.any().item()):
            weights[in_box_idx[local]] = 1.0
            continue
        valid = traj_dists[traj_finite]
        mean_d = float(valid.mean().item())
        if mean_d <= 1e-12:
            weights[in_box_idx[local]] = 1.0
            continue
        traj_weights = torch.where(traj_finite, traj_dists / mean_d, torch.ones_like(traj_dists))
        weights[in_box_idx[local]] = traj_weights
    return weights


def _knn_representative_support(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    trajectory_ids: set[int],
    params: dict[str, float],
    representatives_per_trajectory: int = KNN_REPRESENTATIVES_PER_TRAJECTORY,
) -> torch.Tensor:
    """Return nearest in-window points of kNN-selected trajectories as positive support.

    Labels up to ``representatives_per_trajectory`` points per answer trajectory.
    This keeps the signal much denser than the old top-3 limit while still
    focusing supervision on the points most likely to preserve the kNN answer.
    """
    support = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    time_start = float(params["t_center"] - params["t_half_window"])
    time_end = float(params["t_center"] + params["t_half_window"])
    limit = int(representatives_per_trajectory)

    for trajectory_id in sorted(trajectory_ids):
        if trajectory_id < 0 or trajectory_id >= len(boundaries):
            continue
        start, end = boundaries[trajectory_id]
        if end <= start:
            continue
        trajectory_points = points[start:end]
        in_window = (trajectory_points[:, 0] >= time_start) & (trajectory_points[:, 0] <= time_end)
        candidate_offsets = torch.where(in_window)[0]
        if candidate_offsets.numel() == 0:
            continue
        if limit > 0 and candidate_offsets.numel() > limit:
            candidates = trajectory_points[candidate_offsets]
            distance = _haversine_km(candidates[:, 1], candidates[:, 2], float(params["lat"]), float(params["lon"]))
            distance = distance + 0.001 * torch.abs(candidates[:, 0] - float(params["t_center"]))
            nearest = torch.topk(-distance, k=limit).indices
            candidate_offsets = candidate_offsets[nearest]
        support[start + candidate_offsets] = True

    return support


def _knn_distance_weights(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    trajectory_ids: set[int],
    params: dict[str, float],
) -> torch.Tensor:
    """Per-point weights for kNN labels: inverse distance from each in-window point to the query anchor.

    Used by the ``distance_weighted`` knn-label variant. F1 for kNN only counts whether
    *any* in-window point of an answer-trajectory is retained close enough to the anchor
    to make it into the top-K — so label mass should concentrate on the point most likely
    to be that one (the closest representative) rather than spread evenly across all
    in-window points. Returns a per-point weight tensor; non-trajectory points get 0.
    Inside each trajectory, weights sum to 1 across that trajectory's representatives so
    _add_weighted_hit_label can distribute one trajectory-hit gain by these weights.
    """
    weights = torch.zeros((points.shape[0],), dtype=torch.float32, device=points.device)
    time_start = float(params["t_center"] - params["t_half_window"])
    time_end = float(params["t_center"] + params["t_half_window"])
    limit = int(KNN_REPRESENTATIVES_PER_TRAJECTORY)

    for trajectory_id in sorted(trajectory_ids):
        if trajectory_id < 0 or trajectory_id >= len(boundaries):
            continue
        start, end = boundaries[trajectory_id]
        if end <= start:
            continue
        trajectory_points = points[start:end]
        in_window = (trajectory_points[:, 0] >= time_start) & (trajectory_points[:, 0] <= time_end)
        candidate_offsets = torch.where(in_window)[0]
        if candidate_offsets.numel() == 0:
            continue
        candidates = trajectory_points[candidate_offsets]
        distance = _haversine_km(candidates[:, 1], candidates[:, 2], float(params["lat"]), float(params["lon"]))
        distance = distance + 0.001 * torch.abs(candidates[:, 0] - float(params["t_center"]))
        if limit > 0 and candidate_offsets.numel() > limit:
            nearest = torch.topk(-distance, k=limit).indices
            candidate_offsets = candidate_offsets[nearest]
            distance = distance[nearest]
        # Inverse distance, eps to avoid div-by-zero. Normalise so each trajectory's
        # support points sum to 1 (so _add_weighted_hit_label distributes gain by these).
        inv = 1.0 / (distance + 1e-3)
        weights[start + candidate_offsets] = inv / inv.sum().clamp(min=1e-12)
    return weights


def _similarity_support_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    trajectory_ids: set[int],
    query: dict[str, Any],
    representatives_per_trajectory: int = SIMILARITY_REPRESENTATIVES_PER_TRAJECTORY,
) -> torch.Tensor:
    """Return reference-nearest points for trajectories selected by similarity execution."""
    params = query["params"]
    support = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    time_start = float(params["t_start"])
    time_end = float(params["t_end"])
    reference = torch.tensor(query.get("reference", []), dtype=points.dtype, device=points.device)
    limit = int(representatives_per_trajectory)
    for trajectory_id in sorted(trajectory_ids):
        if trajectory_id < 0 or trajectory_id >= len(boundaries):
            continue
        start, end = boundaries[trajectory_id]
        if end <= start:
            continue
        trajectory_points = points[start:end]
        in_window = (trajectory_points[:, 0] >= time_start) & (trajectory_points[:, 0] <= time_end)
        candidate_offsets = torch.where(in_window)[0]
        if candidate_offsets.numel() == 0:
            continue
        if reference.numel() > 0 and limit > 0 and candidate_offsets.numel() > limit:
            candidates = trajectory_points[candidate_offsets]
            spatial = torch.cdist(candidates[:, 1:3], reference[:, 1:3]).min(dim=1).values
            temporal = torch.cdist(candidates[:, 0:1], reference[:, 0:1]).min(dim=1).values
            radius = max(float(params.get("radius", 1.0)), 1e-6)
            time_span = max(time_end - time_start, 1e-6)
            distance = spatial / radius + 0.25 * temporal / time_span
            candidate_offsets = candidate_offsets[torch.topk(-distance, k=limit).indices]
        support[start + candidate_offsets] = True
    return support


def _cluster_members(labels: Mapping[int, int] | Sequence[int]) -> dict[int, list[int]]:
    """Group non-noise trajectory labels by cluster ID."""
    items = labels.items() if isinstance(labels, Mapping) else enumerate(labels)
    clusters: dict[int, list[int]] = {}
    for trajectory_id, label in items:
        label_value = int(label)
        if label_value == -1:
            continue
        clusters.setdefault(label_value, []).append(int(trajectory_id))
    return clusters


def compute_typed_importance_labels(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    typed_queries: list[dict[str, Any]],
    seed: int,
    similarity_sample_rate: float = 0.70,
    clustering_sample_rate: float = 0.70,
    knn_label_variant: str = "legacy",
    range_label_variant: str = "legacy",
) -> tuple[torch.Tensor, torch.Tensor]:
    """Compute per-point per-type labels as expected query-F1 contribution."""
    n = points.shape[0]
    labels = torch.zeros((n, NUM_QUERY_TYPES), dtype=torch.float32, device=points.device)
    labelled_mask = torch.zeros((n, NUM_QUERY_TYPES), dtype=torch.bool, device=points.device)
    query_counts = torch.zeros((NUM_QUERY_TYPES,), dtype=torch.float32, device=points.device)
    point_trajectory_ids = _trajectory_id_per_point(n, boundaries, points.device)
    trajectories = _trajectories_from_boundaries(points, boundaries)

    # Column 7 of the trajectory feature tensor is turn_score in [0,1] = normalized |Δheading|.
    # Used as a Douglas-Peucker-style shape prior for similarity and clustering branches only.
    # Range is excluded (steals budget from in-box stretches → hurts point recall).
    turn_score = points[:, 7] if points.shape[1] >= 8 else torch.zeros(n, device=points.device)
    TURN_BIAS_ALPHA = 0.05

    for q in typed_queries:
        qtype = q["type"]
        t_idx = QUERY_NAME_TO_ID[qtype]
        params = q["params"]
        query_counts[t_idx] += 1.0

        if qtype == "range":
            box_support = _box_mask(points, params)
            hit_count = int(box_support.sum().item())
            if hit_count > 0:
                base_gain = float(2.0 / (hit_count + 1.0))
                if range_label_variant == "uniform":
                    # Range AnswerF1 is point-recall — every in-box point counts equally.
                    # Uniform per-in-box-point label drops the boundary/proximity bias so
                    # the model isn't pulled toward boundary-only retention which doesn't
                    # match what point F1 actually rewards.
                    labels[box_support, t_idx] += base_gain
                else:
                    bp_weights = _range_boundary_weights(points, box_support, point_trajectory_ids)
                    labels[box_support, t_idx] += base_gain * bp_weights[box_support]

        elif qtype == "knn":
            original_ids = set(execute_typed_query(points, trajectories, q, boundaries))
            gain = _set_query_singleton_gain(original_ids)
            if gain <= 0.0:
                continue
            support = _knn_representative_support(points, boundaries, original_ids, params)
            if knn_label_variant == "distance_weighted":
                # Concentrate label mass on the closest-to-anchor representative within
                # each answer trajectory, since F1 only needs that single point retained.
                weights = _knn_distance_weights(points, boundaries, original_ids, params)
                for trajectory_id in original_ids:
                    trajectory_support = support & (point_trajectory_ids == int(trajectory_id))
                    _add_weighted_hit_label(labels, trajectory_support, t_idx, gain, weights)
            else:
                for trajectory_id in original_ids:
                    trajectory_support = support & (point_trajectory_ids == int(trajectory_id))
                    _add_distributed_hit_label(labels, trajectory_support, t_idx, gain)

        elif qtype == "similarity":
            original_ids = set(execute_typed_query(points, trajectories, q, boundaries))
            gain = _set_query_singleton_gain(original_ids)
            if gain > 0.0:
                similarity_support = _similarity_support_mask(points, boundaries, original_ids, q)
                for trajectory_id in original_ids:
                    support = similarity_support & (point_trajectory_ids == int(trajectory_id))
                    _add_distributed_hit_label(labels, support, t_idx, gain)
                labels[similarity_support, t_idx] += TURN_BIAS_ALPHA * turn_score[similarity_support]

        elif qtype == "clustering":
            original_labels = execute_typed_query(points, trajectories, q, boundaries)
            clusters = _cluster_members(original_labels)
            pair_count = sum(len(members) * (len(members) - 1) // 2 for members in clusters.values())
            if pair_count <= 0:
                continue
            box_support = _box_mask(points, params)
            centroid_weights = _within_box_centroid_weights(points, box_support, point_trajectory_ids)
            clustered_traj_mask = torch.zeros_like(box_support)
            for members in clusters.values():
                degree = len(members) - 1
                if degree <= 0:
                    continue
                gain = float(2.0 * degree / (pair_count + degree))
                for trajectory_id in members:
                    support = box_support & (point_trajectory_ids == int(trajectory_id))
                    _add_weighted_hit_label(labels, support, t_idx, gain, centroid_weights)
                    clustered_traj_mask |= point_trajectory_ids == int(trajectory_id)
            cluster_turn_support = box_support & clustered_traj_mask
            labels[cluster_turn_support, t_idx] += TURN_BIAS_ALPHA * turn_score[cluster_turn_support]

    range_type_idx = QUERY_NAME_TO_ID["range"]
    for type_idx in range(NUM_QUERY_TYPES):
        count = float(query_counts[type_idx].item())
        if count > 0.0:
            # Lever 1.5: sqrt-normalize range labels so per-point magnitudes are large enough
            # for BCE to surface the density prior (hot regions vs cold regions). Preserves
            # the 10x spatial contrast but lifts the absolute label scale by ~sqrt(N) relative
            # to the other types, giving the range head a meaningful gradient.
            divisor = float(count ** 0.5) if type_idx == range_type_idx else count
            labels[:, type_idx] = torch.clamp(labels[:, type_idx] / divisor, 0.0, 1.0)
            labelled_mask[:, type_idx] = True

    return labels, labelled_mask
