"""Typed query F1 metrics and aggregate scoring. See src/evaluation/README.md for details."""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from typing import Hashable, Iterable, Mapping, Sequence

import torch

KM_PER_DEG_LAT = 111.32


def f1_score(r_o: set[Hashable], r_s: set[Hashable]) -> float:
    """Compute F1 agreement between original and simplified query answer sets."""
    if not r_o and not r_s:
        return 1.0
    if not r_o or not r_s:
        return 0.0

    intersection = len(r_s & r_o)
    if intersection == 0:
        return 0.0

    precision = intersection / len(r_s)
    recall = intersection / len(r_o)
    denom = precision + recall
    if denom <= 0.0:
        return 0.0
    return float(2.0 * precision * recall / denom)


def _label_items(labels: Mapping[int, int] | Sequence[int] | torch.Tensor) -> Iterable[tuple[int, int]]:
    """Yield stable trajectory ID and cluster label pairs."""
    if isinstance(labels, Mapping):
        for traj_id, label in labels.items():
            yield int(traj_id), int(label)
        return
    values = labels.tolist() if isinstance(labels, torch.Tensor) else labels
    for traj_id, label in enumerate(values):
        yield int(traj_id), int(label)


def _co_membership_pairs(labels: Mapping[int, int] | Sequence[int] | torch.Tensor) -> set[tuple[int, int]]:
    """Build unordered same-cluster trajectory-pair set, excluding noise label -1."""
    clusters: dict[int, list[int]] = {}
    for traj_id, label in _label_items(labels):
        if label == -1:
            continue
        clusters.setdefault(label, []).append(traj_id)

    pairs: set[tuple[int, int]] = set()
    for members in clusters.values():
        for i, j in combinations(sorted(members), 2):
            pairs.add((i, j))
    return pairs


def clustering_f1(
    labels_o: Mapping[int, int] | Sequence[int] | torch.Tensor,
    labels_s: Mapping[int, int] | Sequence[int] | torch.Tensor,
) -> float:
    """Compute F1 over clustering co-membership pair sets."""
    return f1_score(_co_membership_pairs(labels_o), _co_membership_pairs(labels_s))


@dataclass
class MethodEvaluation:
    """Container for method-level aggregate and per-type F1 scores. See src/evaluation/README.md for details."""

    aggregate_f1: float
    per_type_f1: dict[str, float]
    aggregate_combined_f1: float = 0.0
    per_type_combined_f1: dict[str, float] = field(default_factory=dict)
    compression_ratio: float = 0.0
    latency_ms: float = 0.0
    avg_retained_point_gap: float = 0.0
    avg_retained_point_gap_norm: float = 0.0
    max_retained_point_gap: float = 0.0
    geometric_distortion: dict[str, float] = field(default_factory=dict)
    avg_length_preserved: float = 1.0
    length_preservation_aggregates: dict[str, float] = field(default_factory=dict)
    combined_query_shape_score: float = 0.0
    retained_mask: torch.Tensor | None = None


def _trajectory_sed_ped_km(
    times: torch.Tensor,
    lats: torch.Tensor,
    lons: torch.Tensor,
    retained: torch.Tensor,
) -> tuple[float, float, float, float, int]:
    """Return (sum_sed_km, max_sed_km, sum_ped_km, max_ped_km, removed_count) for one trajectory.

    SED is the time-synchronous distance (Meratnia & de By 2004): for each removed
    point at time t, distance to the linearly-interpolated (in time) position on the
    simplified polyline between the two retained points that bracket t.
    PED is the perpendicular distance (Imai & Iri 1988; what Douglas-Peucker minimizes):
    distance from each removed point to the chord segment of the bracketing retained
    points. Both use a local equirectangular projection to km, accurate for the short
    inter-point segments typical of AIS at Danish latitudes.
    """
    n = times.numel()
    if n < 2:
        return 0.0, 0.0, 0.0, 0.0, 0
    retained_idx = torch.where(retained)[0]
    if retained_idx.numel() < 2:
        return 0.0, 0.0, 0.0, 0.0, 0
    removed_idx = torch.where(~retained)[0]
    if removed_idx.numel() == 0:
        return 0.0, 0.0, 0.0, 0.0, 0

    pos = torch.searchsorted(retained_idx, removed_idx)
    valid = (pos > 0) & (pos < retained_idx.numel())
    if not valid.any():
        return 0.0, 0.0, 0.0, 0.0, 0
    pos = pos[valid]
    removed_idx = removed_idx[valid]
    left_idx = retained_idx[pos - 1]
    right_idx = retained_idx[pos]

    t_l = times[left_idx]
    t_r = times[right_idx]
    t_p = times[removed_idx]
    dt = (t_r - t_l).clamp(min=1e-9)
    alpha = ((t_p - t_l) / dt).clamp(min=0.0, max=1.0)
    interp_lat = lats[left_idx] + alpha * (lats[right_idx] - lats[left_idx])
    interp_lon = lons[left_idx] + alpha * (lons[right_idx] - lons[left_idx])

    cos_lat = torch.cos(torch.deg2rad(lats[removed_idx]))
    dx_km = (lons[removed_idx] - interp_lon) * cos_lat * KM_PER_DEG_LAT
    dy_km = (lats[removed_idx] - interp_lat) * KM_PER_DEG_LAT
    sed_km = torch.sqrt(dx_km * dx_km + dy_km * dy_km)

    cos_lat_left = torch.cos(torch.deg2rad(lats[left_idx]))
    bx_km = (lons[right_idx] - lons[left_idx]) * cos_lat_left * KM_PER_DEG_LAT
    by_km = (lats[right_idx] - lats[left_idx]) * KM_PER_DEG_LAT
    px_km = (lons[removed_idx] - lons[left_idx]) * cos_lat_left * KM_PER_DEG_LAT
    py_km = (lats[removed_idx] - lats[left_idx]) * KM_PER_DEG_LAT
    chord_len = torch.sqrt(bx_km * bx_km + by_km * by_km).clamp(min=1e-9)
    cross = bx_km * py_km - by_km * px_km
    ped_km = torch.abs(cross) / chord_len

    return (
        float(sed_km.sum().item()),
        float(sed_km.max().item()),
        float(ped_km.sum().item()),
        float(ped_km.max().item()),
        int(removed_idx.numel()),
    )


def _per_trajectory_length_preserved(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
) -> tuple[list[float], list[float], list[float]]:
    """Return per-trajectory (orig_km, simp_km, ratio) for trajectories with non-zero length.

    A single shared helper feeds both the length-weighted aggregate
    (compute_average_length_loss) and the distributional summary
    (compute_length_preservation_aggregates), so the two views always agree on
    which trajectories were eligible. Trajectories shorter than 2 points or with
    zero original km are excluded — there is no meaningful "preservation" to report.
    """
    points_cpu = points.detach().cpu()
    mask_cpu = retained_mask.detach().cpu().bool()
    lats = points_cpu[:, 1]
    lons = points_cpu[:, 2]

    orig_kms: list[float] = []
    simp_kms: list[float] = []
    ratios: list[float] = []
    for s, e in boundaries:
        if e - s < 2:
            continue
        traj_lat = lats[s:e]
        traj_lon = lons[s:e]
        orig_km = _polyline_length_km(traj_lat, traj_lon)
        if orig_km <= 1e-9:
            continue
        traj_mask = mask_cpu[s:e]
        if int(traj_mask.sum().item()) >= 2:
            simp_km = _polyline_length_km(traj_lat[traj_mask], traj_lon[traj_mask])
        else:
            simp_km = 0.0
        orig_kms.append(float(orig_km))
        simp_kms.append(float(simp_km))
        ratios.append(float(max(0.0, min(1.0, simp_km / orig_km))))
    return orig_kms, simp_kms, ratios


def compute_average_length_loss(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
) -> float:
    """F1-style length-preservation score in [0, 1].

    1.0 = simplified polyline preserves total path length perfectly.
    0.0 = simplified polyline collapsed (lost all length).

    Aggregated as ratio-of-total-km (sum_simp_km / sum_orig_km) so long trajectories
    are weighted by their length and short / near-stationary trajectories don't
    dominate the score. This matches the headline avg_orig_km / avg_simp_km numbers
    printed alongside it.
    """
    orig_kms, simp_kms, _ = _per_trajectory_length_preserved(points, boundaries, retained_mask)
    total_orig_km = sum(orig_kms)
    total_simp_km = sum(simp_kms)
    if total_orig_km <= 1e-9:
        return 1.0
    return float(max(0.0, min(1.0, total_simp_km / total_orig_km)))


def compute_length_preservation_aggregates(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
) -> dict[str, float]:
    """Whole-eval-set length-preservation summary, one number per aggregate.

    Complements the length-weighted ratio in compute_average_length_loss by
    surfacing distributional views that single-number ratio hides. All values
    are computed once over the full set of evaluable trajectories (those with
    >= 2 points and non-zero original km), independent of query type.

    Keys (each in [0, 1] except n_trajectories):
        weighted_ratio   sum_simp_km / sum_orig_km — same as avg_length_preserved
        mean_per_traj    Unweighted mean per-trajectory ratio (every traj equal weight)
        median_per_traj  Median per-trajectory ratio (robust to outliers)
        p10_per_traj     10th-percentile ratio (lower tail / typical worst case)
        min_per_traj     Worst trajectory ratio
        frac_above_0p9   Fraction of trajectories with ratio >= 0.9
        frac_above_0p95  Fraction of trajectories with ratio >= 0.95
        n_trajectories   Count of evaluable trajectories
    """
    _, _, ratios = _per_trajectory_length_preserved(points, boundaries, retained_mask)
    n = len(ratios)
    if n == 0:
        return {
            "weighted_ratio": 1.0,
            "mean_per_traj": 1.0,
            "median_per_traj": 1.0,
            "p10_per_traj": 1.0,
            "min_per_traj": 1.0,
            "frac_above_0p9": 1.0,
            "frac_above_0p95": 1.0,
            "n_trajectories": 0,
        }
    sorted_ratios = sorted(ratios)
    if n % 2 == 1:
        median = sorted_ratios[n // 2]
    else:
        median = 0.5 * (sorted_ratios[n // 2 - 1] + sorted_ratios[n // 2])
    p10_idx = max(0, min(n - 1, int(round(0.1 * (n - 1)))))
    p10 = sorted_ratios[p10_idx]
    return {
        "weighted_ratio": compute_average_length_loss(points, boundaries, retained_mask),
        "mean_per_traj": float(sum(ratios) / n),
        "median_per_traj": float(median),
        "p10_per_traj": float(p10),
        "min_per_traj": float(sorted_ratios[0]),
        "frac_above_0p9": float(sum(1 for r in ratios if r >= 0.9) / n),
        "frac_above_0p95": float(sum(1 for r in ratios if r >= 0.95) / n),
        "n_trajectories": n,
    }


def _polyline_length_km(lats: torch.Tensor, lons: torch.Tensor) -> float:
    """Sum of haversine distances between consecutive (lat, lon) points in km."""
    n = lats.numel()
    if n < 2:
        return 0.0
    radius_km = 6371.0
    lat_rad = torch.deg2rad(lats)
    lon_rad = torch.deg2rad(lons)
    dlat = lat_rad[1:] - lat_rad[:-1]
    dlon = lon_rad[1:] - lon_rad[:-1]
    a = torch.sin(dlat / 2.0) ** 2 + torch.cos(lat_rad[:-1]) * torch.cos(lat_rad[1:]) * torch.sin(dlon / 2.0) ** 2
    c = 2.0 * torch.atan2(torch.sqrt(a), torch.sqrt(torch.clamp(1.0 - a, min=1e-9)))
    return float((radius_km * c).sum().item())


def compute_geometric_distortion(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
) -> dict[str, float]:
    """Average and max SED / PED in km across every removed point in the eval set.

    SED  = time-synchronous distance (Meratnia & de By 2004).
    PED  = perpendicular distance to chord (Imai & Iri 1988; Douglas-Peucker target).
    Reports point-weighted averages (sum over all removed points / number of removed
    points) and global maxima across all trajectories. Removed points that fall outside
    the retained span (before the first or after the last retained point in a trajectory)
    are excluded because no valid chord exists for them.
    """
    points_cpu = points.detach().cpu()
    mask_cpu = retained_mask.detach().cpu().bool()
    times_all = points_cpu[:, 0]
    lats_all = points_cpu[:, 1]
    lons_all = points_cpu[:, 2]

    total_sed = 0.0
    total_ped = 0.0
    max_sed = 0.0
    max_ped = 0.0
    total_removed = 0
    for s, e in boundaries:
        if e - s < 2:
            continue
        traj_sed_sum, traj_sed_max, traj_ped_sum, traj_ped_max, count = _trajectory_sed_ped_km(
            times_all[s:e], lats_all[s:e], lons_all[s:e], mask_cpu[s:e]
        )
        total_sed += traj_sed_sum
        total_ped += traj_ped_sum
        if traj_sed_max > max_sed:
            max_sed = traj_sed_max
        if traj_ped_max > max_ped:
            max_ped = traj_ped_max
        total_removed += count

    if total_removed == 0:
        return {"avg_sed_km": 0.0, "max_sed_km": 0.0, "avg_ped_km": 0.0, "max_ped_km": 0.0, "removed_points": 0}
    return {
        "avg_sed_km": float(total_sed / total_removed),
        "max_sed_km": float(max_sed),
        "avg_ped_km": float(total_ped / total_removed),
        "max_ped_km": float(max_ped),
        "removed_points": int(total_removed),
    }
