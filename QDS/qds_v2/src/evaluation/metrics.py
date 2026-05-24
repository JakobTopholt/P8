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


def compute_in_query_length_retention(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    support_mask: torch.Tensor,
) -> dict[str, float]:
    """Length retention restricted to the in-query segment of each touched trajectory.

    For each trajectory that contains at least one in-query point (per support_mask),
    the segment evaluated runs from `first_in_query_point - 1` to `last_in_query_point + 1`
    (clipped at trajectory ends). This is the third scope: not the whole eval set,
    not the whole trajectory of any-touched-query, but the slice the query actually
    cares about plus one anchor point on each side so the polyline length is
    well-defined (the chord that enters/leaves the query region is counted).

    Returns avg_orig_km, avg_simp_km, and the ratio (length_retention) over the
    set of in-query segments. Trajectories with <2 in-query points or zero
    original km are excluded.
    """
    points_cpu = points.detach().cpu()
    retained_cpu = retained_mask.detach().cpu().bool()
    support_cpu = support_mask.detach().cpu().bool()
    lats = points_cpu[:, 1]
    lons = points_cpu[:, 2]

    orig_kms: list[float] = []
    simp_kms: list[float] = []
    for s, e in boundaries:
        if e - s < 2:
            continue
        traj_support = support_cpu[s:e]
        if int(traj_support.sum().item()) <= 0:
            continue
        in_idx = torch.where(traj_support)[0]
        first_in = int(in_idx.min().item())
        last_in = int(in_idx.max().item())
        seg_start = max(0, first_in - 1)
        seg_end = min((e - s) - 1, last_in + 1)
        if seg_end - seg_start < 1:
            continue
        # Original polyline within the segment (always uses every original point in segment).
        seg_lat = lats[s + seg_start : s + seg_end + 1]
        seg_lon = lons[s + seg_start : s + seg_end + 1]
        orig_km = _polyline_length_km(seg_lat, seg_lon)
        if orig_km <= 1e-9:
            continue
        # Simplified polyline within the segment: only retained points in [seg_start, seg_end].
        seg_retained = retained_cpu[s + seg_start : s + seg_end + 1]
        if int(seg_retained.sum().item()) >= 2:
            simp_km = _polyline_length_km(seg_lat[seg_retained], seg_lon[seg_retained])
        else:
            simp_km = 0.0
        orig_kms.append(float(orig_km))
        simp_kms.append(float(simp_km))

    n = len(orig_kms)
    if n == 0:
        return {"avg_orig_km": 0.0, "avg_simp_km": 0.0, "retention": 1.0, "n_segments": 0}
    sum_orig = sum(orig_kms)
    sum_simp = sum(simp_kms)
    retention = 1.0 if sum_orig <= 1e-9 else max(0.0, min(1.0, sum_simp / sum_orig))
    return {
        "avg_orig_km": float(sum_orig / n),
        "avg_simp_km": float(sum_simp / n),
        "retention": float(retention),
        "n_segments": n,
    }


def compute_full_trajectory_lengths_km(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
) -> list[float]:
    """Full polyline length (km) of every trajectory, indexed by trajectory id.

    Precompute once and reuse across per-query Scope-3 bucketing so each query
    doesn't recompute trajectory lengths.
    """
    points_cpu = points.detach().cpu()
    lats = points_cpu[:, 1]
    lons = points_cpu[:, 2]
    out: list[float] = []
    for s, e in boundaries:
        if e - s < 2:
            out.append(0.0)
            continue
        out.append(float(_polyline_length_km(lats[s:e], lons[s:e])))
    return out


# Default trajectory-length buckets (km), high-to-low. Each in-query segment is
# assigned to the bucket of its PARENT trajectory's full length.
SCOPE3_LENGTH_BUCKETS: list[tuple[float, float, str]] = [
    (200.0, float("inf"), "200+"),
    (150.0, 200.0, "150-200"),
    (100.0, 150.0, "100-150"),
    (50.0, 100.0, "50-100"),
    (25.0, 50.0, "25-50"),
    (15.0, 25.0, "15-25"),
    (0.0, 15.0, "0-15"),
]


def _bucket_label_for_length(full_km: float, buckets: list[tuple[float, float, str]]) -> str | None:
    """Return the bucket label whose [lo, hi) range contains full_km, else None."""
    for lo, hi, label in buckets:
        if full_km >= lo and full_km < hi:
            return label
    return None


def count_trajectories_by_length_bucket(
    full_traj_km: list[float],
    buckets: list[tuple[float, float, str]] = SCOPE3_LENGTH_BUCKETS,
    min_km: float = 1e-9,
) -> dict[str, int]:
    """Count how many trajectories in the dataset fall into each length bucket.

    Query-independent dataset distribution: each trajectory is counted once in
    the bucket of its full polyline length. Trajectories with ~zero length
    (``< min_km``) are skipped so near-stationary tracks don't inflate the
    smallest bucket. Use this to contextualize Scope-3 per-bucket retention
    (a bucket with few trajectories has noisier retention).
    """
    counts: dict[str, int] = {label: 0 for _, _, label in buckets}
    for km in full_traj_km:
        if float(km) < min_km:
            continue
        label = _bucket_label_for_length(float(km), buckets)
        if label is not None:
            counts[label] += 1
    return counts


def compute_in_query_length_retention_bucketed(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    support_mask: torch.Tensor,
    full_traj_km: list[float],
    buckets: list[tuple[float, float, str]] = SCOPE3_LENGTH_BUCKETS,
) -> dict[str, dict[str, float]]:
    """Scope-3 in-query retention split into buckets by PARENT trajectory length.

    Same in-query segment definition as ``compute_in_query_length_retention``
    (``first_in - 1`` to ``last_in + 1``), but each segment's orig/simp km is
    accumulated into the bucket of its parent trajectory's FULL length
    (``full_traj_km[tid]``). Returns per-bucket running sums so a caller can
    aggregate across many queries before computing the ratio.

    Returns ``{bucket_label: {sum_orig_km, sum_simp_km, n_segments}}``.
    """
    points_cpu = points.detach().cpu()
    retained_cpu = retained_mask.detach().cpu().bool()
    support_cpu = support_mask.detach().cpu().bool()
    lats = points_cpu[:, 1]
    lons = points_cpu[:, 2]

    out: dict[str, dict[str, float]] = {
        label: {"sum_orig_km": 0.0, "sum_simp_km": 0.0, "n_segments": 0} for _, _, label in buckets
    }
    for tid, (s, e) in enumerate(boundaries):
        if e - s < 2:
            continue
        traj_support = support_cpu[s:e]
        if int(traj_support.sum().item()) <= 0:
            continue
        in_idx = torch.where(traj_support)[0]
        first_in = int(in_idx.min().item())
        last_in = int(in_idx.max().item())
        seg_start = max(0, first_in - 1)
        seg_end = min((e - s) - 1, last_in + 1)
        if seg_end - seg_start < 1:
            continue
        seg_lat = lats[s + seg_start : s + seg_end + 1]
        seg_lon = lons[s + seg_start : s + seg_end + 1]
        orig_km = _polyline_length_km(seg_lat, seg_lon)
        if orig_km <= 1e-9:
            continue
        seg_retained = retained_cpu[s + seg_start : s + seg_end + 1]
        if int(seg_retained.sum().item()) >= 2:
            simp_km = _polyline_length_km(seg_lat[seg_retained], seg_lon[seg_retained])
        else:
            simp_km = 0.0
        label = _bucket_label_for_length(float(full_traj_km[tid]) if tid < len(full_traj_km) else 0.0, buckets)
        if label is None:
            continue
        out[label]["sum_orig_km"] += float(orig_km)
        out[label]["sum_simp_km"] += float(simp_km)
        out[label]["n_segments"] += 1
    return out


def compute_length_preserved_for_trajectories(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    trajectory_indices: list[int] | None = None,
) -> dict[str, float]:
    """Length-weighted preservation for an explicit subset of trajectories.

    Used when restricting the metric to query-relevant trajectories — pass the
    list of trajectory indices that any eval query touches, and this returns
    the same sum_simp_km / sum_orig_km ratio computed only over those. With
    trajectory_indices=None falls back to all trajectories (matches the global
    compute_average_length_loss).
    """
    if trajectory_indices is None:
        sub_boundaries = boundaries
    else:
        sub_boundaries = [boundaries[i] for i in trajectory_indices if 0 <= i < len(boundaries)]
        if not sub_boundaries:
            return {"weighted_ratio": 1.0, "sum_orig_km": 0.0, "sum_simp_km": 0.0, "n_trajectories": 0}
    orig_kms, simp_kms, _ = _per_trajectory_length_preserved(points, sub_boundaries, retained_mask)
    total_orig = sum(orig_kms)
    total_simp = sum(simp_kms)
    ratio = 1.0 if total_orig <= 1e-9 else max(0.0, min(1.0, total_simp / total_orig))
    return {
        "weighted_ratio": float(ratio),
        "sum_orig_km": float(total_orig),
        "sum_simp_km": float(total_simp),
        "n_trajectories": len(orig_kms),
    }


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
