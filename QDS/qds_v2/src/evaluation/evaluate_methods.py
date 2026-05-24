"""Method evaluation and fixed-width results table helpers. See src/evaluation/README.md for details."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import torch

from src.evaluation.baselines import Method
from src.evaluation.metrics import (
    MethodEvaluation,
    clustering_f1,
    compute_average_length_loss,
    compute_geometric_distortion,
    compute_in_query_length_retention,
    compute_length_preservation_aggregates,
    compute_length_preserved_for_trajectories,
    f1_score,
)
from src.queries.query_executor import execute_typed_query

POINT_AWARE_KNN_REPRESENTATIVES_PER_TRAJECTORY = 64
POINT_AWARE_SIMILARITY_REPRESENTATIVES_PER_TRAJECTORY = 64


def _split_by_boundaries(points: torch.Tensor, boundaries: list[tuple[int, int]]) -> list[torch.Tensor]:
    """Split flattened points into trajectory list by boundaries. See src/evaluation/README.md for details."""
    return [points[s:e] for s, e in boundaries]


def _range_box_mask(points: torch.Tensor, params: dict[str, float]) -> torch.Tensor:
    """Return point-level hits inside a range query box."""
    return (
        (points[:, 1] >= params["lat_min"])
        & (points[:, 1] <= params["lat_max"])
        & (points[:, 2] >= params["lon_min"])
        & (points[:, 2] <= params["lon_max"])
        & (points[:, 0] >= params["t_start"])
        & (points[:, 0] <= params["t_end"])
    )


def _range_point_f1(points: torch.Tensor, simplified: torch.Tensor, params: dict[str, float]) -> float:
    """Compute range F1 over point hits instead of trajectory-presence hits."""
    full_mask = _range_box_mask(points, params)
    simplified_mask = _range_box_mask(simplified, params)

    full_points = {tuple(row) for row in points[full_mask].tolist()}
    simplified_points = {tuple(row) for row in simplified[simplified_mask].tolist()}

    full_hits = len(full_points)
    simplified_hits = len(simplified_points)
    if full_hits == 0 and simplified_hits == 0:
        return 1.0
    if full_hits == 0 or simplified_hits == 0:
        return 0.0

    true_positives = len(full_points.intersection(simplified_points))
    precision = float(true_positives / simplified_hits)
    recall = float(true_positives / full_hits)
    if precision + recall == 0.0:
        return 0.0
    return float((2.0 * precision * recall) / (precision + recall))


def _trajectory_id_per_point(n_points: int, boundaries: list[tuple[int, int]], device: torch.device) -> torch.Tensor:
    trajectory_ids = torch.full((n_points,), -1, dtype=torch.long, device=device)
    for trajectory_id, (start, end) in enumerate(boundaries):
        if end > start:
            trajectory_ids[start:end] = int(trajectory_id)
    return trajectory_ids


def _ids_mask(point_trajectory_ids: torch.Tensor, trajectory_ids: set[int]) -> torch.Tensor:
    mask = torch.zeros_like(point_trajectory_ids, dtype=torch.bool)
    for trajectory_id in trajectory_ids:
        mask |= point_trajectory_ids == int(trajectory_id)
    return mask


def _haversine_km(lat1: torch.Tensor, lon1: torch.Tensor, lat2: float, lon2: float) -> torch.Tensor:
    radius_km = 6371.0
    lat1_rad = torch.deg2rad(lat1)
    lon1_rad = torch.deg2rad(lon1)
    lat2_rad = torch.deg2rad(torch.tensor(lat2, dtype=lat1.dtype, device=lat1.device))
    lon2_rad = torch.deg2rad(torch.tensor(lon2, dtype=lon1.dtype, device=lon1.device))
    delta_lat = lat1_rad - lat2_rad
    delta_lon = lon1_rad - lon2_rad
    haversine = (
        torch.sin(delta_lat / 2.0) ** 2
        + torch.cos(lat1_rad) * torch.cos(lat2_rad) * torch.sin(delta_lon / 2.0) ** 2
    )
    central_angle = 2.0 * torch.atan2(torch.sqrt(haversine), torch.sqrt(torch.clamp(1.0 - haversine, min=1e-9)))
    return radius_km * central_angle


def _point_subset_f1(retained_mask: torch.Tensor, support_mask: torch.Tensor) -> float:
    full_hits = int(support_mask.sum().item())
    if full_hits <= 0:
        return 1.0
    retained_hits = int((retained_mask & support_mask).sum().item())
    if retained_hits <= 0:
        return 0.0
    recall = float(retained_hits / full_hits)
    return float((2.0 * recall) / (1.0 + recall))


def _knn_representative_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    trajectory_ids: set[int],
    params: dict[str, float],
    representatives_per_trajectory: int = POINT_AWARE_KNN_REPRESENTATIVES_PER_TRAJECTORY,
) -> torch.Tensor:
    support = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    time_start = float(params["t_center"] - params["t_half_window"])
    time_end = float(params["t_center"] + params["t_half_window"])
    limit = int(representatives_per_trajectory)
    for trajectory_id in sorted(trajectory_ids):
        if trajectory_id < 0 or trajectory_id >= len(boundaries):
            continue
        start, end = boundaries[trajectory_id]
        trajectory_points = points[start:end]
        in_window = (trajectory_points[:, 0] >= time_start) & (trajectory_points[:, 0] <= time_end)
        candidate_offsets = torch.where(in_window)[0]
        if candidate_offsets.numel() == 0:
            continue
        if limit > 0 and candidate_offsets.numel() > limit:
            candidates = trajectory_points[candidate_offsets]
            distance = _haversine_km(candidates[:, 1], candidates[:, 2], float(params["lat"]), float(params["lon"]))
            distance = distance + 0.001 * torch.abs(candidates[:, 0] - float(params["t_center"]))
            candidate_offsets = candidate_offsets[torch.topk(-distance, k=limit).indices]
        support[start + candidate_offsets] = True
    return support


def _similarity_support_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    trajectory_ids: set[int],
    query: dict,
    representatives_per_trajectory: int = POINT_AWARE_SIMILARITY_REPRESENTATIVES_PER_TRAJECTORY,
) -> torch.Tensor:
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


def _clustering_support_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    labels: list[int],
    params: dict[str, float],
) -> torch.Tensor:
    clustered_ids = {trajectory_id for trajectory_id, label in enumerate(labels) if int(label) != -1}
    if not clustered_ids:
        return torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    point_trajectory_ids = _trajectory_id_per_point(points.shape[0], boundaries, points.device)
    return _range_box_mask(points, params) & _ids_mask(point_trajectory_ids, clustered_ids)


@dataclass
class QueryEvalCache:
    """Precomputed per-query data that doesn't depend on simplification method.

    Eliminates the redundant computation in evaluate_method when multiple
    methods (MLQDS, uniform, DP, Oracle) are scored on the same query set.
    Without this each method recomputes full_res and support_mask for every
    query, paying 4× the cost.

    full_res:  ground-truth answer set per query (kNN/similarity/clustering).
               Range is not cached (cheap point_f1 instead).
    support:   per-point support mask per query (used by combined F1 product).
    """

    full_res: list[Any] = field(default_factory=list)       # per-query answer set
    support: list[torch.Tensor] = field(default_factory=list)  # per-query mask


def precompute_query_cache(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    typed_queries: list[dict],
) -> QueryEvalCache:
    """Build a QueryEvalCache for sharing across multiple method evaluations."""
    full_traj = _split_by_boundaries(points, boundaries)
    cache = QueryEvalCache()
    empty_mask = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    for query in typed_queries:
        qtype = query["type"]
        if qtype == "range":
            # Range uses _range_point_f1 which doesn't need full_res. Cache a
            # no-op placeholder to keep the per-query index aligned.
            cache.full_res.append(None)
            cache.support.append(empty_mask)
            continue
        full_res = execute_typed_query(points, full_traj, query, boundaries)
        cache.full_res.append(full_res)
        if qtype == "knn":
            cache.support.append(_knn_representative_mask(points, boundaries, set(full_res), query["params"]))
        elif qtype == "similarity":
            cache.support.append(_similarity_support_mask(points, boundaries, set(full_res), query))
        elif qtype == "clustering":
            cache.support.append(_clustering_support_mask(points, boundaries, list(full_res), query["params"]))
        else:
            cache.support.append(empty_mask)
    return cache


def score_retained_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    typed_queries: list[dict],
    workload_mix: dict[str, float],
    cache: QueryEvalCache | None = None,
) -> tuple[float, dict[str, float], float, dict[str, float]]:
    """Score a precomputed retained mask with the final query-F1 semantics.

    Returns (aggregate_answer_f1, per_type_answer_f1, aggregate_combined,
    per_type_combined). The "answer" variant uses pure set/cluster F1 between
    queries on the full vs simplified data (the natural, defensible metric).
    The "combined" variant is the legacy answer_f1 * point_subset_f1 product
    kept for diagnostic comparison; it double-penalizes a method that returns
    the right answer set via different points than ground truth's "support".

    cache: optional precomputed QueryEvalCache to skip per-query full_res and
    support_mask recomputation across methods. Pass None to compute inline.
    """
    simplified = points[retained_mask]
    simp_boundaries: list[tuple[int, int]] = []
    cursor = 0
    for start, end in boundaries:
        n = int(retained_mask[start:end].sum().item())
        simp_boundaries.append((cursor, cursor + n))
        cursor += n
    simp_traj = _split_by_boundaries(simplified, simp_boundaries)
    # Only need full_traj if cache is None (we'll be computing full_res inline)
    full_traj = _split_by_boundaries(points, boundaries) if cache is None else None

    answer_scores: dict[str, list[float]] = {"range": [], "knn": [], "similarity": [], "clustering": []}
    combined_scores: dict[str, list[float]] = {"range": [], "knn": [], "similarity": [], "clustering": []}
    for q_idx, query in enumerate(typed_queries):
        qtype = query["type"]
        if qtype == "range":
            point_f1 = _range_point_f1(points, simplified, query["params"])
            answer_scores[qtype].append(point_f1)
            combined_scores[qtype].append(point_f1)
            continue

        if cache is not None:
            full_res = cache.full_res[q_idx]
            support = cache.support[q_idx]
        else:
            full_res = execute_typed_query(points, full_traj, query, boundaries)
            if qtype == "knn":
                support = _knn_representative_mask(points, boundaries, set(full_res), query["params"])
            elif qtype == "similarity":
                support = _similarity_support_mask(points, boundaries, set(full_res), query)
            elif qtype == "clustering":
                support = _clustering_support_mask(points, boundaries, list(full_res), query["params"])
            else:
                support = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)

        simp_res = execute_typed_query(simplified, simp_traj, query, simp_boundaries)
        if qtype == "knn":
            ans = f1_score(set(full_res), set(simp_res))
            answer_scores[qtype].append(ans)
            combined_scores[qtype].append(ans * _point_subset_f1(retained_mask, support))
        elif qtype == "similarity":
            ans = f1_score(set(full_res), set(simp_res))
            answer_scores[qtype].append(ans)
            combined_scores[qtype].append(ans * _point_subset_f1(retained_mask, support))
        elif qtype == "clustering":
            ans = clustering_f1(full_res, simp_res)
            answer_scores[qtype].append(ans)
            combined_scores[qtype].append(ans * _point_subset_f1(retained_mask, support))

    per_type_answer = {name: (sum(v) / len(v) if v else 0.0) for name, v in answer_scores.items()}
    per_type_combined = {name: (sum(v) / len(v) if v else 0.0) for name, v in combined_scores.items()}
    weight_sum = sum(workload_mix.values()) if workload_mix else 0.0
    if weight_sum <= 0.0:
        normalized_mix = {name: 1.0 / 4.0 for name in per_type_answer}
    else:
        normalized_mix = {name: workload_mix.get(name, 0.0) / weight_sum for name in per_type_answer}
    aggregate_answer = sum(normalized_mix[name] * per_type_answer[name] for name in per_type_answer)
    aggregate_combined = sum(normalized_mix[name] * per_type_combined[name] for name in per_type_combined)
    return float(aggregate_answer), per_type_answer, float(aggregate_combined), per_type_combined


def _retained_point_gap_stats(
    retained_mask: torch.Tensor,
    boundaries: list[tuple[int, int]],
) -> tuple[float, float, float]:
    """Return average and max original-index gaps between retained points."""
    total_gap = 0.0
    total_norm_gap = 0.0
    max_gap = 0.0
    gap_count = 0
    for start, end in boundaries:
        n = int(end - start)
        if n <= 1:
            continue
        offsets = torch.where(retained_mask[start:end])[0].float()
        if offsets.numel() < 2:
            continue
        gaps = offsets[1:] - offsets[:-1]
        denom = float(max(1, n - 1))
        total_gap += float(gaps.sum().item())
        total_norm_gap += float((gaps / denom).sum().item())
        max_gap = max(max_gap, float(gaps.max().item()))
        gap_count += int(gaps.numel())

    if gap_count <= 0:
        return 0.0, 0.0, 0.0
    return float(total_gap / gap_count), float(total_norm_gap / gap_count), float(max_gap)


def evaluate_method(
    method: Method,
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    typed_queries: list[dict],
    workload_mix: dict[str, float],
    compression_ratio: float,
    return_mask: bool = False,
    cache: QueryEvalCache | None = None,
) -> MethodEvaluation:
    """Evaluate one simplification method on typed queries at matched ratio. See src/evaluation/README.md for details."""
    t0 = time.time()
    retained_mask = method.simplify(points, boundaries, compression_ratio)
    latency_ms = (time.time() - t0) * 1000.0

    aggregate, per_type, aggregate_combined, per_type_combined = score_retained_mask(
        points=points,
        boundaries=boundaries,
        retained_mask=retained_mask,
        typed_queries=typed_queries,
        workload_mix=workload_mix,
        cache=cache,
    )
    comp = float(retained_mask.float().mean().item())
    avg_gap, avg_norm_gap, max_gap = _retained_point_gap_stats(retained_mask, boundaries)
    geometric = compute_geometric_distortion(points, boundaries, retained_mask)
    length_aggs = compute_length_preservation_aggregates(points, boundaries, retained_mask)
    avg_length_preserved = float(length_aggs.get("weighted_ratio", 1.0))
    combined = float(aggregate) * max(0.0, min(1.0, avg_length_preserved))

    return MethodEvaluation(
        aggregate_f1=float(aggregate),
        per_type_f1=per_type,
        aggregate_combined_f1=float(aggregate_combined),
        per_type_combined_f1=per_type_combined,
        compression_ratio=comp,
        latency_ms=latency_ms,
        avg_retained_point_gap=avg_gap,
        avg_retained_point_gap_norm=avg_norm_gap,
        max_retained_point_gap=max_gap,
        geometric_distortion=geometric,
        avg_length_preserved=avg_length_preserved,
        length_preservation_aggregates=length_aggs,
        combined_query_shape_score=combined,
        retained_mask=retained_mask if return_mask else None,
    )


def print_method_comparison_table(results: dict[str, MethodEvaluation]) -> str:
    """Render fixed-width method comparison table with per-type rows. See src/evaluation/README.md for details.

    Shows two F1 columns: AnswerF1 = pure set/cluster F1 between full and
    simplified queries (the natural metric); CombinedF1 = legacy
    answer_f1 * point_subset_f1 product kept for diagnostic comparison.
    """
    col1, col2, col3, col4, col5, col6, col7 = 24, 12, 12, 12, 12, 14, 12
    lines = []
    header = (
        f"{'Method':<{col1}}"
        f"{'AnswerF1':>{col2}}"
        f"{'CombinedF1':>{col3}}"
        f"{'Compression':>{col4}}"
        f"{'AvgPtGap':>{col5}}"
        f"{'Latency(ms)':>{col6}}"
        f"{'Type':>{col7}}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for name, metrics in results.items():
        lines.append(
            f"{name:<{col1}}"
            f"{metrics.aggregate_f1:>{col2}.6f}"
            f"{metrics.aggregate_combined_f1:>{col3}.6f}"
            f"{metrics.compression_ratio:>{col4}.4f}"
            f"{metrics.avg_retained_point_gap:>{col5}.2f}"
            f"{metrics.latency_ms:>{col6}.2f}"
            f"{'all':>{col7}}"
        )
        for t_name in ("range", "knn", "similarity", "clustering"):
            lines.append(
                f"{'  - ' + t_name:<{col1}}"
                f"{metrics.per_type_f1.get(t_name, 0.0):>{col2}.6f}"
                f"{metrics.per_type_combined_f1.get(t_name, 0.0):>{col3}.6f}"
                f"{'':>{col4}}"
                f"{'':>{col5}}"
                f"{'':>{col6}}"
                f"{t_name:>{col7}}"
            )

    def _rel_pct(diff: float, baseline: float) -> str:
        """Format a percentage of baseline, with safe div-by-zero handling."""
        if abs(baseline) < 1e-9:
            return "  n/a"
        return f"{100.0 * diff / baseline:+.1f}%"

    mlqds = results.get("MLQDS")
    uniform = results.get("uniform")
    dp = results.get("DouglasPeucker")
    if mlqds is not None and (uniform is not None or dp is not None):
        lines.append("-" * len(header))
        lines.append(f"{'Diff vs MLQDS (AnswerF1 / CombinedF1; abs and % vs baseline)':<{col1}}")
        for ref_name, ref in (("uniform", uniform), ("DouglasPeucker", dp)):
            if ref is None:
                continue
            agg_ans = mlqds.aggregate_f1 - ref.aggregate_f1
            agg_comb = mlqds.aggregate_combined_f1 - ref.aggregate_combined_f1
            agg_ans_pct = _rel_pct(agg_ans, ref.aggregate_f1)
            agg_comb_pct = _rel_pct(agg_comb, ref.aggregate_combined_f1)
            label = f"  vs {ref_name}"
            lines.append(
                f"{label:<{col1}}"
                f"{agg_ans:>+{col2}.6f}"
                f"{agg_comb:>+{col3}.6f}"
                f"{'':>{col4}}"
                f"{'':>{col5}}"
                f"{'':>{col6}}"
                f"{'all':>{col7}}"
            )
            lines.append(
                f"{'      (% vs baseline)':<{col1}}"
                f"{agg_ans_pct:>{col2}}"
                f"{agg_comb_pct:>{col3}}"
                f"{'':>{col4}}"
                f"{'':>{col5}}"
                f"{'':>{col6}}"
                f"{'all':>{col7}}"
            )
            for t_name in ("range", "knn", "similarity", "clustering"):
                ref_ans = ref.per_type_f1.get(t_name, 0.0)
                ref_comb = ref.per_type_combined_f1.get(t_name, 0.0)
                t_ans = mlqds.per_type_f1.get(t_name, 0.0) - ref_ans
                t_comb = mlqds.per_type_combined_f1.get(t_name, 0.0) - ref_comb
                t_ans_pct = _rel_pct(t_ans, ref_ans)
                t_comb_pct = _rel_pct(t_comb, ref_comb)
                lines.append(
                    f"{'    - ' + t_name:<{col1}}"
                    f"{t_ans:>+{col2}.6f}"
                    f"{t_comb:>+{col3}.6f}"
                    f"{'':>{col4}}"
                    f"{'':>{col5}}"
                    f"{'':>{col6}}"
                    f"{t_name:>{col7}}"
                )
                lines.append(
                    f"{'      (% vs baseline)':<{col1}}"
                    f"{t_ans_pct:>{col2}}"
                    f"{t_comb_pct:>{col3}}"
                    f"{'':>{col4}}"
                    f"{'':>{col5}}"
                    f"{'':>{col6}}"
                    f"{t_name:>{col7}}"
                )
    return "\n".join(lines)


def print_geometric_distortion_table(results: dict[str, MethodEvaluation]) -> str:
    """Render geometric-distortion table (SED + PED in km; lower is better).

    SED (Meratnia & de By 2004) is the time-synchronous distance from each
    removed point to the linearly-interpolated chord between the two retained
    points that bracket it in time. PED (Imai & Iri 1988; what Douglas-Peucker
    minimises) is the perpendicular distance to the chord. Both are reported as
    point-weighted averages over every removed point, plus the global maximum.
    Length-retention numbers live in the dedicated 3-scope tables.
    """
    col1, col2, col3, col4, col5 = 24, 11, 11, 11, 11
    header = (
        f"{'Method':<{col1}}"
        f"{'AvgSED_km':>{col2}}"
        f"{'MaxSED_km':>{col3}}"
        f"{'AvgPED_km':>{col4}}"
        f"{'MaxPED_km':>{col5}}"
    )
    lines = [header, "-" * len(header)]
    for name, metrics in results.items():
        g = metrics.geometric_distortion or {}
        lines.append(
            f"{name:<{col1}}"
            f"{g.get('avg_sed_km', 0.0):>{col2}.4f}"
            f"{g.get('max_sed_km', 0.0):>{col3}.2f}"
            f"{g.get('avg_ped_km', 0.0):>{col4}.4f}"
            f"{g.get('max_ped_km', 0.0):>{col5}.2f}"
        )
    return "\n".join(lines)


def _query_geom_summary(query: dict) -> dict[str, float | None]:
    """Unified spatial/temporal summary for any query type.

    Every query gets center_lat/center_lon/t_center plus, where defined, the
    bbox edges (lat/lon min-max) and the time window (t_start/t_end). knn and
    similarity have point-anchored regions so bbox edges come back as None;
    clustering has no native time window. The CSV writer emits None as empty
    strings so downstream plotting code can branch on which fields are present.
    """
    qtype = query["type"]
    p = query["params"]
    out: dict[str, float | None] = {
        "center_lat": None, "center_lon": None,
        "lat_min": None, "lat_max": None, "lon_min": None, "lon_max": None,
        "t_start": None, "t_end": None, "t_center": None,
        "radius": None, "t_half_window": None,
    }
    if qtype in ("range", "clustering"):
        out["lat_min"] = float(p["lat_min"])
        out["lat_max"] = float(p["lat_max"])
        out["lon_min"] = float(p["lon_min"])
        out["lon_max"] = float(p["lon_max"])
        out["center_lat"] = (out["lat_min"] + out["lat_max"]) / 2.0
        out["center_lon"] = (out["lon_min"] + out["lon_max"]) / 2.0
        if qtype == "range":
            out["t_start"] = float(p["t_start"])
            out["t_end"] = float(p["t_end"])
            out["t_center"] = (out["t_start"] + out["t_end"]) / 2.0
    elif qtype == "knn":
        out["center_lat"] = float(p["lat"])
        out["center_lon"] = float(p["lon"])
        out["t_center"] = float(p["t_center"])
        out["t_half_window"] = float(p["t_half_window"])
        out["t_start"] = out["t_center"] - out["t_half_window"]
        out["t_end"] = out["t_center"] + out["t_half_window"]
    elif qtype == "similarity":
        out["center_lat"] = float(p["lat_query_centroid"])
        out["center_lon"] = float(p["lon_query_centroid"])
        out["radius"] = float(p.get("radius", 0.0))
        out["t_start"] = float(p["t_start"])
        out["t_end"] = float(p["t_end"])
        out["t_center"] = (out["t_start"] + out["t_end"]) / 2.0
    return out


def _query_support_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    query: dict,
    full_traj: list[torch.Tensor],
) -> torch.Tensor:
    """Return the per-point support mask for one eval query.

    Range: in-box points. kNN/similarity: representative points within the
    query window for each trajectory in the full answer set. Clustering: in-box
    points belonging to clustered (non-noise) trajectories. The mask defines
    "data points the query touches" used by per-query length-preservation and
    by the per-type query_points CSVs.
    """
    qtype = query["type"]
    if qtype == "range":
        return _range_box_mask(points, query["params"])
    full_res = execute_typed_query(points, full_traj, query, boundaries)
    if qtype == "knn":
        return _knn_representative_mask(points, boundaries, set(full_res), query["params"])
    if qtype == "similarity":
        return _similarity_support_mask(points, boundaries, set(full_res), query)
    if qtype == "clustering":
        return _clustering_support_mask(points, boundaries, list(full_res), query["params"])
    return torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)


def _query_answer_f1(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    query: dict,
    full_traj: list[torch.Tensor],
    simp_points: torch.Tensor,
    simp_boundaries: list[tuple[int, int]],
    simp_traj: list[torch.Tensor],
) -> float:
    """Compute the same per-query AnswerF1 used by score_retained_mask."""
    qtype = query["type"]
    if qtype == "range":
        return _range_point_f1(points, simp_points, query["params"])
    full_res = execute_typed_query(points, full_traj, query, boundaries)
    simp_res = execute_typed_query(simp_points, simp_traj, query, simp_boundaries)
    if qtype == "clustering":
        return clustering_f1(full_res, simp_res)
    return f1_score(set(full_res), set(simp_res))


def _trajectories_touching_mask(mask: torch.Tensor, boundaries: list[tuple[int, int]]) -> list[int]:
    """Return indices of trajectories with at least one True point in mask."""
    out: list[int] = []
    for i, (s, e) in enumerate(boundaries):
        if e > s and bool(mask[s:e].any().item()):
            out.append(i)
    return out


def build_query_support_masks(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    typed_queries: list[dict],
) -> list[torch.Tensor]:
    """One per-point boolean mask per query (same order as typed_queries)."""
    full_traj = _split_by_boundaries(points, boundaries)
    return [_query_support_mask(points, boundaries, q, full_traj) for q in typed_queries]


def compute_eval_query_length_preservation(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_masks_by_method: dict[str, torch.Tensor],
    typed_queries: list[dict],
) -> dict[str, dict[str, float]]:
    """Length preservation restricted to trajectories any eval query touches.

    Returns per-method dicts with weighted_ratio / sum_orig_km / sum_simp_km /
    n_trajectories. Useful when the eval workload only covers part of the
    fleet — preserving km on stationary/never-queried boats inflates the
    whole-set number, this strips them out.
    """
    full_traj = _split_by_boundaries(points, boundaries)
    union = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
    for q in typed_queries:
        union |= _query_support_mask(points, boundaries, q, full_traj)
    traj_idx = _trajectories_touching_mask(union, boundaries)
    return {
        name: compute_length_preserved_for_trajectories(points, boundaries, mask, trajectory_indices=traj_idx)
        for name, mask in retained_masks_by_method.items()
    }


def compute_per_query_diagnostics(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_masks_by_method: dict[str, torch.Tensor],
    typed_queries: list[dict],
) -> list[dict]:
    """Per-query AnswerF1 + length preservation + point counts, all methods.

    For each eval query the returned dict carries the support mask size,
    per-method retained-vs-removed counts inside the support, per-method
    AnswerF1, and per-method weighted length preservation restricted to the
    trajectories the query touches. This is the source of truth for the
    top-15 best/worst tables and for any per-query downstream report.
    """
    full_traj = _split_by_boundaries(points, boundaries)
    out: list[dict] = []
    method_names = list(retained_masks_by_method.keys())
    simp_cache: dict[str, tuple[torch.Tensor, list[tuple[int, int]], list[torch.Tensor]]] = {}
    for name, mask in retained_masks_by_method.items():
        simp_pts = points[mask]
        simp_bnd: list[tuple[int, int]] = []
        cursor = 0
        for s, e in boundaries:
            n = int(mask[s:e].sum().item())
            simp_bnd.append((cursor, cursor + n))
            cursor += n
        simp_cache[name] = (simp_pts, simp_bnd, _split_by_boundaries(simp_pts, simp_bnd))

    for q_idx, query in enumerate(typed_queries):
        support = _query_support_mask(points, boundaries, query, full_traj)
        n_in = int(support.sum().item())
        traj_idx = _trajectories_touching_mask(support, boundaries)
        per_method: dict[str, dict[str, float]] = {}
        for name in method_names:
            mask = retained_masks_by_method[name]
            simp_pts, simp_bnd, simp_traj = simp_cache[name]
            n_retained_in = int((mask & support).sum().item())
            # Scope 2 view: full polyline of trajectories the query touches.
            length_full = compute_length_preserved_for_trajectories(points, boundaries, mask, trajectory_indices=traj_idx)
            # Scope 3 view: in-query segments (first_in-1 to last_in+1).
            length_inquery = compute_in_query_length_retention(points, boundaries, mask, support)
            f1 = _query_answer_f1(points, boundaries, mask, query, full_traj, simp_pts, simp_bnd, simp_traj)
            per_method[name] = {
                "answer_f1": float(f1),
                # Backwards-compat field — Scope-2 retention (full trajectories of touched queries).
                "length_preserved": float(length_full["weighted_ratio"]),
                # New: Scope-3 retention restricted to in-query segments only.
                "in_query_retention": float(length_inquery["retention"]),
                "in_query_orig_km": float(length_inquery["avg_orig_km"] * max(1, length_inquery["n_segments"])),
                "in_query_simp_km": float(length_inquery["avg_simp_km"] * max(1, length_inquery["n_segments"])),
                "in_query_n_segments": int(length_inquery["n_segments"]),
                "n_retained_in_query": n_retained_in,
                "n_removed_in_query": int(n_in - n_retained_in),
                "n_trajectories_touched": int(length_full["n_trajectories"]),
            }
        out.append({
            "query_idx": q_idx,
            "type": str(query["type"]),
            "n_points_in_query": n_in,
            "n_trajectories_touched": len(traj_idx),
            "geom": _query_geom_summary(query),
            "per_method": per_method,
        })
    return out


def _gap_vs_baselines(diag_per_method: dict[str, dict[str, float]]) -> tuple[float, str]:
    """For one query, return (gap, best_baseline_name) where gap = MLQDS_F1 - best_baseline_F1.

    Best baseline is the higher of uniform vs DouglasPeucker — beating that
    is the bar the model has to clear. Gap > 0 means MLQDS won, gap < 0 means
    the better of the two baselines beat MLQDS.
    """
    if "MLQDS" not in diag_per_method:
        return 0.0, ""
    mlqds = diag_per_method["MLQDS"]["answer_f1"]
    candidates = [(name, diag_per_method[name]["answer_f1"]) for name in ("uniform", "DouglasPeucker") if name in diag_per_method]
    if not candidates:
        return 0.0, ""
    best_name, best_f1 = max(candidates, key=lambda x: x[1])
    return float(mlqds - best_f1), best_name


def _retention_gap_vs_baselines(diag_per_method: dict[str, dict[str, float]]) -> tuple[float, str]:
    """For one query, return (gap, best_baseline_name) where gap = MLQDS_in_query_retention − best_baseline.

    Mirrors _gap_vs_baselines but on Scope-3 length retention. Used to rank
    queries when the user cares about length preservation more than F1.
    """
    if "MLQDS" not in diag_per_method:
        return 0.0, ""
    mlqds = diag_per_method["MLQDS"].get("in_query_retention", 0.0)
    candidates = [(name, diag_per_method[name].get("in_query_retention", 0.0)) for name in ("uniform", "DouglasPeucker") if name in diag_per_method]
    if not candidates:
        return 0.0, ""
    best_name, best_r = max(candidates, key=lambda x: x[1])
    return float(mlqds - best_r), best_name


def print_top_queries_table(diagnostics: list[dict], k: int = 15, mode: str = "best") -> str:
    """Render the top-K best (or worst) queries by MLQDS Scope-3 retention gap.

    "best" = top-K queries where MLQDS-in-query-retention exceeds the better of
    uniform/DP by the most. "worst" = bottom-K (where MLQDS loses by the most).
    Each row reports per-query AnswerF1 (for context, doesn't drive the rank)
    and the in-query retention numbers that drive the ranking.
    """
    scored = [(d, *_retention_gap_vs_baselines(d.get("per_method", {}))) for d in diagnostics]
    if mode == "best":
        ranked = sorted(scored, key=lambda x: x[1], reverse=True)[:k]
        title = f"Top {k} BEST queries (MLQDS in-query retention gap vs best baseline; positive = MLQDS won)"
    else:
        ranked = sorted(scored, key=lambda x: x[1])[:k]
        title = f"Top {k} WORST queries (MLQDS in-query retention gap vs best baseline; negative = MLQDS lost)"

    cols = ["rank", "qid", "type", "gap_R", "R_M", "R_U", "R_DP", "F1_M", "F1_U", "F1_DP", "in_q", "rm_M", "rm_U", "rm_DP"]
    widths = [4, 5, 11, 8, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6]
    header = "  ".join(f"{c:>{w}}" for c, w in zip(cols, widths))
    lines = [title, header, "-" * len(header)]
    for rank, (d, gap, _) in enumerate(ranked, start=1):
        pm = d.get("per_method", {})
        m = pm.get("MLQDS", {})
        u = pm.get("uniform", {})
        dp = pm.get("DouglasPeucker", {})
        row = [
            f"{rank}", f"{d['query_idx']}", d["type"],
            f"{gap:+.4f}",
            f"{m.get('in_query_retention', 0.0):.3f}",
            f"{u.get('in_query_retention', 0.0):.3f}",
            f"{dp.get('in_query_retention', 0.0):.3f}",
            f"{m.get('answer_f1', 0.0):.4f}",
            f"{u.get('answer_f1', 0.0):.4f}",
            f"{dp.get('answer_f1', 0.0):.4f}",
            f"{d['n_points_in_query']}",
            f"{m.get('n_removed_in_query', 0)}",
            f"{u.get('n_removed_in_query', 0)}",
            f"{dp.get('n_removed_in_query', 0)}",
        ]
        lines.append("  ".join(f"{v:>{w}}" for v, w in zip(row, widths)))
    return "\n".join(lines)


def write_top_queries_csv(diagnostics: list[dict], out_path: str, k: int = 15, mode: str = "best") -> None:
    """Persist the top-K best (or worst) per-query rows ranked by Scope-3 retention gap.

    Ranking metric: MLQDS_in_query_retention − max(uniform, DP) in_query_retention.
    F1 columns are kept as context but don't drive the ordering. The CSV carries
    the unified spatial/temporal summary (center_lat/center_lon/t_center plus
    type-specific extents) so plotting code can render each query's region on a
    map without re-parsing the original query dicts.
    """
    scored = [(d, *_retention_gap_vs_baselines(d.get("per_method", {}))) for d in diagnostics]
    if mode == "best":
        ranked = sorted(scored, key=lambda x: x[1], reverse=True)[:k]
    else:
        ranked = sorted(scored, key=lambda x: x[1])[:k]
    cols = [
        "rank", "query_idx", "type", "retention_gap_vs_best_baseline",
        "MLQDS_in_query_retention", "uniform_in_query_retention", "DP_in_query_retention",
        "MLQDS_F1", "uniform_F1", "DP_F1",
        "n_points_in_query", "n_trajectories_touched",
        "MLQDS_n_removed", "uniform_n_removed", "DP_n_removed",
        "MLQDS_in_query_orig_km", "MLQDS_in_query_simp_km",
        "uniform_in_query_orig_km", "uniform_in_query_simp_km",
        "DP_in_query_orig_km", "DP_in_query_simp_km",
        "center_lat", "center_lon", "t_center",
        "lat_min", "lat_max", "lon_min", "lon_max",
        "t_start", "t_end", "radius", "t_half_window",
    ]

    def _fmt(v: float | int | None, places: int = 6) -> str:
        if v is None:
            return ""
        if isinstance(v, int):
            return str(v)
        return f"{float(v):.{places}f}"

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(",".join(cols) + "\n")
        for rank, (d, gap, _) in enumerate(ranked, start=1):
            pm = d.get("per_method", {})
            m = pm.get("MLQDS", {}); u = pm.get("uniform", {}); dp = pm.get("DouglasPeucker", {})
            g = d.get("geom", {})
            row = [
                str(rank), str(d.get("query_idx", "")), str(d.get("type", "")),
                _fmt(gap, 6),
                _fmt(m.get("in_query_retention"), 6), _fmt(u.get("in_query_retention"), 6), _fmt(dp.get("in_query_retention"), 6),
                _fmt(m.get("answer_f1"), 6), _fmt(u.get("answer_f1"), 6), _fmt(dp.get("answer_f1"), 6),
                str(int(d.get("n_points_in_query", 0))), str(int(d.get("n_trajectories_touched", 0))),
                str(int(m.get("n_removed_in_query", 0))), str(int(u.get("n_removed_in_query", 0))), str(int(dp.get("n_removed_in_query", 0))),
                _fmt(m.get("in_query_orig_km"), 4), _fmt(m.get("in_query_simp_km"), 4),
                _fmt(u.get("in_query_orig_km"), 4), _fmt(u.get("in_query_simp_km"), 4),
                _fmt(dp.get("in_query_orig_km"), 4), _fmt(dp.get("in_query_simp_km"), 4),
                _fmt(g.get("center_lat"), 6), _fmt(g.get("center_lon"), 6), _fmt(g.get("t_center"), 3),
                _fmt(g.get("lat_min"), 6), _fmt(g.get("lat_max"), 6), _fmt(g.get("lon_min"), 6), _fmt(g.get("lon_max"), 6),
                _fmt(g.get("t_start"), 3), _fmt(g.get("t_end"), 3),
                _fmt(g.get("radius"), 6), _fmt(g.get("t_half_window"), 3),
            ]
            f.write(",".join(row) + "\n")


def _retention_rows(
    method_aggs: dict[str, dict[str, float]],
    n_field: str,
    n_label: str,
) -> str:
    """Shared 4-column rendering for the 3 length-retention scope tables."""
    col1, col2, col3, col4, col5 = 24, 13, 13, 11, 8
    header = (
        f"{'Method':<{col1}}"
        f"{'avg_orig_km':>{col2}}"
        f"{'avg_simp_km':>{col3}}"
        f"{'Retention':>{col4}}"
        f"{n_label:>{col5}}"
    )
    lines = [header, "-" * len(header)]
    for name, agg in method_aggs.items():
        lines.append(
            f"{name:<{col1}}"
            f"{agg.get('avg_orig_km', 0.0):>{col2}.4f}"
            f"{agg.get('avg_simp_km', 0.0):>{col3}.4f}"
            f"{agg.get('retention', 0.0):>{col4}.4f}"
            f"{int(agg.get(n_field, 0)):>{col5}d}"
        )
    return "\n".join(lines)


def print_length_retention_whole_set_table(
    results: dict[str, MethodEvaluation],
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
) -> str:
    """Length retention scope 1: every trajectory ≥2 points in the eval set.

    Per method: avg_orig_km, avg_simp_km, retention (= sum_simp_km / sum_orig_km;
    1.0 = perfect). Higher retention is better. NTraj is the count of
    trajectories that contributed (length ≥ 2 points and non-zero km).
    """
    aggs: dict[str, dict[str, float]] = {}
    for name, metrics in results.items():
        if metrics.retained_mask is None:
            aggs[name] = {"avg_orig_km": 0.0, "avg_simp_km": 0.0, "retention": metrics.avg_length_preserved, "n_trajectories": 0}
            continue
        agg = compute_length_preserved_for_trajectories(points, boundaries, metrics.retained_mask)
        n = int(agg.get("n_trajectories", 0))
        aggs[name] = {
            "avg_orig_km": (agg["sum_orig_km"] / n) if n > 0 else 0.0,
            "avg_simp_km": (agg["sum_simp_km"] / n) if n > 0 else 0.0,
            "retention": float(agg["weighted_ratio"]),
            "n_trajectories": n,
        }
    return "Length retention — Scope 1: whole eval set (every trajectory ≥2 points)\n" + _retention_rows(aggs, "n_trajectories", "NTraj")


def print_length_retention_eval_query_table(per_method_aggs: dict[str, dict[str, float]]) -> str:
    """Length retention scope 2: trajectories any eval query touches (full polyline)."""
    aggs: dict[str, dict[str, float]] = {}
    for name, agg in per_method_aggs.items():
        n = int(agg.get("n_trajectories", 0))
        aggs[name] = {
            "avg_orig_km": float(agg.get("sum_orig_km", 0.0)) / n if n > 0 else 0.0,
            "avg_simp_km": float(agg.get("sum_simp_km", 0.0)) / n if n > 0 else 0.0,
            "retention": float(agg.get("weighted_ratio", 0.0)),
            "n_trajectories": n,
        }
    return "Length retention — Scope 2: trajectories any eval query touches (full polyline)\n" + _retention_rows(aggs, "n_trajectories", "NTraj")


def aggregate_in_query_retention_across_diagnostics(
    diagnostics: list[dict],
    method_names: list[str],
) -> dict[str, dict[str, float]]:
    """Sum per-query Scope-3 numbers into per-method aggregates."""
    out: dict[str, dict[str, float]] = {n: {"sum_orig_km": 0.0, "sum_simp_km": 0.0, "n_segments": 0} for n in method_names}
    for d in diagnostics:
        for name in method_names:
            pm = d.get("per_method", {}).get(name, {})
            out[name]["sum_orig_km"] += float(pm.get("in_query_orig_km", 0.0))
            out[name]["sum_simp_km"] += float(pm.get("in_query_simp_km", 0.0))
            out[name]["n_segments"] += int(pm.get("in_query_n_segments", 0))
    return out


def print_length_retention_in_query_table(in_query_aggs: dict[str, dict[str, float]]) -> str:
    """Length retention scope 3: in-query segments only (first_in-1 to last_in+1)."""
    aggs: dict[str, dict[str, float]] = {}
    for name, agg in in_query_aggs.items():
        n = int(agg.get("n_segments", 0))
        sum_orig = float(agg.get("sum_orig_km", 0.0))
        sum_simp = float(agg.get("sum_simp_km", 0.0))
        retention = 1.0 if sum_orig <= 1e-9 else max(0.0, min(1.0, sum_simp / sum_orig))
        aggs[name] = {
            "avg_orig_km": sum_orig / n if n > 0 else 0.0,
            "avg_simp_km": sum_simp / n if n > 0 else 0.0,
            "retention": retention,
            "n_segments": n,
        }
    return "Length retention — Scope 3: in-query segments (first_in-1 to last_in+1)\n" + _retention_rows(aggs, "n_segments", "NSeg")


def print_length_retention_in_query_by_length_table(
    bucketed_aggs: dict[str, dict[str, dict[str, float]]],
    bucket_order: list[str],
    traj_counts: dict[str, int] | None = None,
) -> str:
    """Render Scope-3 in-query retention split by parent-trajectory length bucket.

    ``bucketed_aggs`` maps method -> bucket_label -> {sum_orig_km, sum_simp_km,
    n_segments}. One sub-table is printed per length bucket so MLQDS / uniform /
    DP can be compared within each trajectory-length class.

    ``traj_counts`` (optional) maps bucket_label -> number of trajectories in
    the dataset within that length range; shown in the bucket header as NTraj
    so a reader can tell how many distinct trajectories back each bucket's
    retention (NSeg = in-query segments, which can exceed NTraj when a
    trajectory is touched by several queries).
    """
    lines: list[str] = [
        "Length retention — Scope 3 by trajectory length (in-query segments, bucketed by full trajectory km)",
        "NTraj = trajectories in this length range (dataset); NSeg = in-query segments scored",
    ]
    for bucket in bucket_order:
        aggs: dict[str, dict[str, float]] = {}
        for name, per_bucket in bucketed_aggs.items():
            agg = per_bucket.get(bucket, {"sum_orig_km": 0.0, "sum_simp_km": 0.0, "n_segments": 0})
            n = int(agg.get("n_segments", 0))
            sum_orig = float(agg.get("sum_orig_km", 0.0))
            sum_simp = float(agg.get("sum_simp_km", 0.0))
            retention = 1.0 if sum_orig <= 1e-9 else max(0.0, min(1.0, sum_simp / sum_orig))
            aggs[name] = {
                "avg_orig_km": sum_orig / n if n > 0 else 0.0,
                "avg_simp_km": sum_simp / n if n > 0 else 0.0,
                "retention": retention,
                "n_segments": n,
            }
        n_any = max((int(aggs[n_]["n_segments"]) for n_ in aggs), default=0)
        ntraj = int(traj_counts.get(bucket, 0)) if traj_counts is not None else None
        ntraj_str = f"  NTraj={ntraj}" if ntraj is not None else ""
        lines.append(f"\n  [{bucket} km]{ntraj_str}  (NSeg={n_any})")
        lines.append(_retention_rows(aggs, "n_segments", "NSeg"))
    return "\n".join(lines)


def print_shift_table(shift_grid: dict[str, dict[str, float]]) -> str:
    """Render train-mix to eval-mix aggregate F1 matrix table. See src/evaluation/README.md for details."""
    eval_cols = sorted({k for row in shift_grid.values() for k in row.keys()})
    col_w = 22
    header_label = "Train\\Eval"
    line = f"{header_label:<{col_w}}" + "".join(f"{c:>{col_w}}" for c in eval_cols)
    out = [line, "-" * len(line)]
    for train_name in sorted(shift_grid.keys()):
        row = f"{train_name:<{col_w}}"
        for eval_name in eval_cols:
            val = shift_grid[train_name].get(eval_name, float("nan"))
            row += f"{val:>{col_w}.4f}"
        out.append(row)
    return "\n".join(out)
