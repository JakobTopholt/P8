"""Method evaluation and fixed-width results table helpers. See src/evaluation/README.md for details."""

from __future__ import annotations

import time

import torch

from src.evaluation.baselines import Method
from src.evaluation.metrics import (
    MethodEvaluation,
    clustering_f1,
    compute_average_length_loss,
    compute_geometric_distortion,
    compute_length_preservation_aggregates,
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


def score_retained_mask(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    retained_mask: torch.Tensor,
    typed_queries: list[dict],
    workload_mix: dict[str, float],
) -> tuple[float, dict[str, float], float, dict[str, float]]:
    """Score a precomputed retained mask with the final query-F1 semantics.

    Returns (aggregate_answer_f1, per_type_answer_f1, aggregate_combined,
    per_type_combined). The "answer" variant uses pure set/cluster F1 between
    queries on the full vs simplified data (the natural, defensible metric).
    The "combined" variant is the legacy answer_f1 * point_subset_f1 product
    kept for diagnostic comparison; it double-penalizes a method that returns
    the right answer set via different points than ground truth's "support".
    """
    simplified = points[retained_mask]
    full_traj = _split_by_boundaries(points, boundaries)
    simp_boundaries: list[tuple[int, int]] = []
    cursor = 0
    for start, end in boundaries:
        n = int(retained_mask[start:end].sum().item())
        simp_boundaries.append((cursor, cursor + n))
        cursor += n
    simp_traj = _split_by_boundaries(simplified, simp_boundaries)

    answer_scores: dict[str, list[float]] = {"range": [], "knn": [], "similarity": [], "clustering": []}
    combined_scores: dict[str, list[float]] = {"range": [], "knn": [], "similarity": [], "clustering": []}
    for query in typed_queries:
        qtype = query["type"]
        if qtype == "range":
            point_f1 = _range_point_f1(points, simplified, query["params"])
            answer_scores[qtype].append(point_f1)
            combined_scores[qtype].append(point_f1)
            continue

        full_res = execute_typed_query(points, full_traj, query, boundaries)
        simp_res = execute_typed_query(simplified, simp_traj, query, simp_boundaries)
        if qtype == "knn":
            ans = f1_score(set(full_res), set(simp_res))
            support = _knn_representative_mask(points, boundaries, set(full_res), query["params"])
            answer_scores[qtype].append(ans)
            combined_scores[qtype].append(ans * _point_subset_f1(retained_mask, support))
        elif qtype == "similarity":
            ans = f1_score(set(full_res), set(simp_res))
            support = _similarity_support_mask(points, boundaries, set(full_res), query)
            answer_scores[qtype].append(ans)
            combined_scores[qtype].append(ans * _point_subset_f1(retained_mask, support))
        elif qtype == "clustering":
            ans = clustering_f1(full_res, simp_res)
            support = _clustering_support_mask(points, boundaries, list(full_res), query["params"])
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
    """Render geometric-distortion + shape-aware utility comparison.

    SED (Meratnia & de By 2004) and PED (Imai & Iri 1988; what Douglas-Peucker
    minimises) are reported in km — lower is better. LengthPres is the fraction of
    total path length preserved (sum_simp_km / sum_orig_km) in [0, 1] — higher is better.
    F1xLen combines aggregate query F1 with shape preservation: equals F1 when shape
    is perfect (length_preserved=1.0) and 0 when simplified trajectory collapses
    (length_preserved=0.0) — higher is better. Use this column as the single
    shape-aware utility number when comparing methods.
    """
    col1, col2, col3, col4, col5, col6, col7 = 24, 11, 11, 11, 11, 13, 13
    header = (
        f"{'Method':<{col1}}"
        f"{'AvgSED_km':>{col2}}"
        f"{'MaxSED_km':>{col3}}"
        f"{'AvgPED_km':>{col4}}"
        f"{'MaxPED_km':>{col5}}"
        f"{'LengthPres':>{col6}}"
        f"{'F1xLen':>{col7}}"
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
            f"{metrics.avg_length_preserved:>{col6}.4f}"
            f"{metrics.combined_query_shape_score:>{col7}.6f}"
        )
    return "\n".join(lines)


def print_length_preservation_table(results: dict[str, MethodEvaluation]) -> str:
    """Render whole-eval-set length-preservation distributional summary.

    Each row reports one method; every column is a single number computed once
    over all evaluable trajectories (no per-query-type breakdown). Higher is
    better in every column. Weighted is the headline length-weighted ratio
    (sum_simp_km / sum_orig_km, same as LengthPres on the geometric table);
    Mean / Median / P10 / Min summarise the distribution of per-trajectory
    ratios; Frac>=0.9 / Frac>=0.95 are tail-coverage indicators. NTraj is the
    number of trajectories that contributed (length >= 2 points and non-zero km).
    """
    col1, col2, col3, col4, col5, col6, col7, col8, col9 = 24, 11, 11, 11, 11, 11, 11, 11, 8
    header = (
        f"{'Method':<{col1}}"
        f"{'Weighted':>{col2}}"
        f"{'Mean':>{col3}}"
        f"{'Median':>{col4}}"
        f"{'P10':>{col5}}"
        f"{'Min':>{col6}}"
        f"{'Frac>=0.9':>{col7}}"
        f"{'Frac>=.95':>{col8}}"
        f"{'NTraj':>{col9}}"
    )
    lines = [header, "-" * len(header)]
    for name, metrics in results.items():
        a = metrics.length_preservation_aggregates or {}
        lines.append(
            f"{name:<{col1}}"
            f"{a.get('weighted_ratio', metrics.avg_length_preserved):>{col2}.4f}"
            f"{a.get('mean_per_traj', 0.0):>{col3}.4f}"
            f"{a.get('median_per_traj', 0.0):>{col4}.4f}"
            f"{a.get('p10_per_traj', 0.0):>{col5}.4f}"
            f"{a.get('min_per_traj', 0.0):>{col6}.4f}"
            f"{a.get('frac_above_0p9', 0.0):>{col7}.4f}"
            f"{a.get('frac_above_0p95', 0.0):>{col8}.4f}"
            f"{int(a.get('n_trajectories', 0)):>{col9}d}"
        )
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
