"""Helper utilities for the AIS experiment pipeline."""

from __future__ import annotations

import csv
import math
import os
import tempfile

import torch

from src.evaluation.baselines import (
    douglas_peucker,
    random_sampling,
    uniform_temporal_sampling,
)
from src.evaluation.metrics import (
    compression_ratio as compute_compression_ratio,
    compute_query_error,
    query_error,
    query_latency,
)
from src.experiments.experiment_config import (
    BaselineConfig,
    MethodMetrics,
    ModelConfig,
    ModelSimplificationResult,
    QueryConfig,
    TypedQueryWorkload,
    VisualizationConfig,
)
from src.queries.query_generator import (
    generate_aggregation_queries,
    generate_density_biased_queries,
    generate_intersection_queries,
    generate_mixed_queries,
    generate_multi_type_workload,
    generate_nearest_neighbor_queries,
    generate_uniform_queries,
)
from src.simplification.simplify_trajectories import (
    apply_threshold_simplification,
    simplify_trajectories,
)


def _format_query_error_value(err: float) -> str:
    """Format query error for readability across very small and typical values."""
    return f"{err:.4f}" if err >= 1e-4 else f"{err:.3e}"


def _count_removed_overlap_stats(
    points: torch.Tensor,
    removed_mask: torch.Tensor,
    queries: torch.Tensor,
    chunk_size: int,
) -> tuple[int, int]:
    """Count removed points overlapping spatial-only and full spatiotemporal queries."""
    n_points = points.shape[0]
    removed_in_spatial = 0
    removed_in_spatiotemporal = 0

    for start in range(0, n_points, chunk_size):
        end = min(n_points, start + chunk_size)
        chunk = points[start:end]
        removed_chunk = removed_mask[start:end]
        if not removed_chunk.any():
            continue

        spatial_matches = (
            (chunk[:, None, 1] >= queries[None, :, 0])
            & (chunk[:, None, 1] <= queries[None, :, 1])
            & (chunk[:, None, 2] >= queries[None, :, 2])
            & (chunk[:, None, 2] <= queries[None, :, 3])
        )
        spatial_any = spatial_matches.any(dim=1)
        spatiotemporal_any = (
            spatial_matches
            & (chunk[:, None, 0] >= queries[None, :, 4])
            & (chunk[:, None, 0] <= queries[None, :, 5])
        ).any(dim=1)

        removed_in_spatial += int((removed_chunk & spatial_any).sum().item())
        removed_in_spatiotemporal += int((removed_chunk & spatiotemporal_any).sum().item())

    return removed_in_spatial, removed_in_spatiotemporal


def _downsample_trajectories_for_visualization(
    trajectories: list[torch.Tensor],
    max_ships: int,
    max_points_per_ship: int,
) -> list[torch.Tensor]:
    """Create a visualization-friendly subset of trajectories."""
    if len(trajectories) <= max_ships:
        selected = trajectories
    else:
        idx = torch.linspace(0, len(trajectories) - 1, steps=max_ships).long().tolist()
        selected = [trajectories[i] for i in idx]

    reduced: list[torch.Tensor] = []
    for traj in selected:
        if traj.shape[0] > max_points_per_ship:
            step = max(1, math.ceil(traj.shape[0] / max_points_per_ship))
            reduced.append(traj[::step])
        else:
            reduced.append(traj)

    return reduced


def _downsample_points_for_visualization(
    points: torch.Tensor,
    importance: torch.Tensor,
    retained_mask: torch.Tensor,
    scores: torch.Tensor,
    max_points: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Downsample point-level arrays consistently for plotting."""
    n_points = points.shape[0]
    if n_points <= max_points:
        return points, importance, retained_mask, scores

    idx = torch.randperm(n_points, device=points.device)[:max_points]
    idx = torch.sort(idx).values

    return points[idx], importance[idx], retained_mask[idx], scores[idx]


def _clean_csv_output_path(source_csv_path: str) -> str:
    """Build output path for retained-point CSV next to the source file."""
    source_dir = os.path.dirname(source_csv_path) or "."
    source_name = os.path.basename(source_csv_path)
    return os.path.join(source_dir, f"MLClean-{source_name}")


def _save_retained_points_csv(
    retained_points: torch.Tensor,
    output_path: str,
    write_chunk_size: int = 100_000,
) -> None:
    """Save retained points to CSV with schema: timestamp,lat,lon,speed,heading."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    points_cpu = retained_points.detach().cpu()
    points_export = points_cpu[:, :5]
    with open(output_path, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(["timestamp", "lat", "lon", "speed", "heading"])

        for start in range(0, points_export.shape[0], write_chunk_size):
            end = min(points_export.shape[0], start + write_chunk_size)
            writer.writerows(points_export[start:end].tolist())


def _generate_queries_for_workload(
    trajectories: list[torch.Tensor],
    query_cfg: QueryConfig,
) -> torch.Tensor | TypedQueryWorkload:
    """Return queries for the requested workload type."""
    workload = query_cfg.workload

    if workload == "uniform":
        return generate_uniform_queries(
            trajectories,
            n_queries=query_cfg.n_queries,
            spatial_fraction=query_cfg.spatial_fraction,
            temporal_fraction=query_cfg.temporal_fraction,
            spatial_bound_lower_quantile=query_cfg.spatial_lower_quantile,
            spatial_bound_upper_quantile=query_cfg.spatial_upper_quantile,
        )
    if workload == "density":
        return generate_density_biased_queries(
            trajectories,
            n_queries=query_cfg.n_queries,
            spatial_fraction=query_cfg.spatial_fraction,
            temporal_fraction=query_cfg.temporal_fraction,
        )
    if workload == "mixed":
        return generate_mixed_queries(
            trajectories,
            total_queries=query_cfg.n_queries,
            density_ratio=query_cfg.density_ratio,
            spatial_fraction=query_cfg.spatial_fraction,
            temporal_fraction=query_cfg.temporal_fraction,
            spatial_bound_lower_quantile=query_cfg.spatial_lower_quantile,
            spatial_bound_upper_quantile=query_cfg.spatial_upper_quantile,
        )

    range_queries = generate_density_biased_queries(
        trajectories,
        n_queries=query_cfg.n_queries,
        spatial_fraction=query_cfg.spatial_fraction,
        temporal_fraction=query_cfg.temporal_fraction,
    )

    if workload == "intersection":
        typed = generate_intersection_queries(
            trajectories,
            n_queries=query_cfg.n_queries,
            spatial_fraction=query_cfg.spatial_fraction,
            temporal_fraction=query_cfg.temporal_fraction,
        )
    elif workload == "aggregation":
        typed = generate_aggregation_queries(
            trajectories,
            n_queries=query_cfg.n_queries,
            spatial_fraction=query_cfg.spatial_fraction,
            temporal_fraction=query_cfg.temporal_fraction,
            spatial_bound_lower_quantile=query_cfg.spatial_lower_quantile,
            spatial_bound_upper_quantile=query_cfg.spatial_upper_quantile,
        )
    elif workload == "nearest":
        typed = generate_nearest_neighbor_queries(
            trajectories,
            n_queries=query_cfg.n_queries,
        )
    elif workload == "multi":
        typed = generate_multi_type_workload(
            trajectories,
            total_queries=query_cfg.n_queries,
            spatial_fraction=query_cfg.spatial_fraction,
            temporal_fraction=query_cfg.temporal_fraction,
            spatial_bound_lower_quantile=query_cfg.spatial_lower_quantile,
            spatial_bound_upper_quantile=query_cfg.spatial_upper_quantile,
        )
    else:
        raise ValueError(
            f"Unknown workload '{workload}'. Choose from: "
            "uniform, density, mixed, intersection, aggregation, nearest, multi."
        )

    return TypedQueryWorkload(range_queries=range_queries, typed_queries=typed)


def _workload_title(workload: str) -> str:
    """Return a human-readable title string for the given workload type."""
    return {
        "uniform": "Uniform Query Workload",
        "density": "Density-Biased Query Workload",
        "mixed": "Mixed Query Workload",
        "intersection": "Intersection Query Workload",
        "aggregation": "Aggregation Query Workload",
        "nearest": "Nearest-Neighbor Query Workload",
        "multi": "Multi-Type Query Workload",
    }.get(workload, workload.capitalize() + " Query Workload")


def _print_workload_comparison_table(
    all_results: dict[str, dict[str, MethodMetrics]],
) -> None:
    """Print a combined comparison table across multiple workloads."""
    col_w = 20
    col_n = 12
    col_e = 14
    col_r = 16
    col_l = 14
    total_w = col_w + col_n + col_e + col_r + col_l + 5

    print()
    print("=" * total_w)
    print("## Workload Comparison")
    print("=" * total_w)
    header = (
        f"{'Workload':<{col_w}} "
        f"{'Method':<{col_n}} "
        f"{'Query Error':>{col_e}} "
        f"{'Compression Ratio':>{col_r}} "
        f"{'Latency (ms)':>{col_l}}"
    )
    print(header)
    print("-" * total_w)

    for wl_name, methods in all_results.items():
        wl_label = _workload_title(wl_name)
        for method_name, metrics in methods.items():
            err_str = _format_query_error_value(metrics.query_error)
            print(
                f"{wl_label:<{col_w}} "
                f"{method_name:<{col_n}} "
                f"{err_str:>{col_e}} "
                f"{metrics.compression_ratio:>{col_r}.4f} "
                f"{metrics.latency_ms:>{col_l}.4f}"
            )
            wl_label = ""

    print("=" * total_w)


def _resolve_effective_max_train_points(
    n_points: int,
    model_cfg: ModelConfig,
) -> tuple[int | None, bool]:
    """Resolve the effective max training points and whether auto-capping was applied."""
    if model_cfg.max_train_points is not None:
        return model_cfg.max_train_points, False

    if model_cfg.model_max_points is not None and n_points > int(model_cfg.model_max_points):
        return int(model_cfg.model_max_points), True

    return None, False


def _resolve_model_variants(model_type: str) -> list[str]:
    """Return the model variants to execute based on CLI/model config."""
    if model_type == "all":
        return ["baseline", "turn_aware"]
    return [model_type]


_MODEL_LABELS: dict[str, str] = {
    "baseline": "ML QDS",
    "turn_aware": "ML QDS (turn-aware)",
}


def find_optimal_threshold(
    scores: torch.Tensor,
    points: torch.Tensor,
    queries: torch.Tensor,
    max_query_error: float,
    boundaries: list[tuple[int, int]],
    min_points_per_trajectory: int = 3,
    max_search_iterations: int = 20,
    error_tolerance: float = 1e-3,
) -> tuple[float, float, float]:
    """Find the highest threshold that stays within the query-error budget."""
    low = float(scores.min().item())
    high = float(scores.max().item())

    from src.queries.query_executor import run_queries as _run_queries
    query_point_chunk_size = 50_000
    query_query_chunk_size = 128
    original_results = _run_queries(
        points,
        queries,
        point_chunk_size=query_point_chunk_size,
        query_chunk_size=query_query_chunk_size,
    )

    best_threshold = low
    best_mean_err: float
    best_max_err: float

    init_simplified, _ = apply_threshold_simplification(
        points, scores, low, boundaries, min_points_per_trajectory
    )
    best_mean_err, best_max_err = compute_query_error(
        points,
        init_simplified,
        queries,
        original_results=original_results,
        point_chunk_size=query_point_chunk_size,
        query_chunk_size=query_query_chunk_size,
    )

    for iteration in range(max_search_iterations):
        mid = (low + high) / 2.0
        simplified, _ = apply_threshold_simplification(
            points, scores, mid, boundaries, min_points_per_trajectory
        )
        mean_err, max_err = compute_query_error(
            points,
            simplified,
            queries,
            original_results=original_results,
            point_chunk_size=query_point_chunk_size,
            query_chunk_size=query_query_chunk_size,
        )

        if mean_err <= max_query_error + error_tolerance:
            best_threshold = mid
            best_mean_err = mean_err
            best_max_err = max_err
            low = mid
        else:
            high = mid

        if (high - low) < 1e-9:
            break

    return best_threshold, best_mean_err, best_max_err


def _simplify_with_model(
    points: torch.Tensor,
    queries: torch.Tensor,
    importance: torch.Tensor,
    trained_model: object,
    model_variant: str,
    model_cfg: ModelConfig,
    trajectory_boundaries: list[tuple[int, int]],
) -> ModelSimplificationResult:
    """Run trajectory simplification for one trained model variant."""
    eff_turn_bias = model_cfg.turn_bias_weight if model_variant == "turn_aware" else 0.0
    label = _MODEL_LABELS.get(model_variant, f"ML QDS ({model_variant})")

    if model_cfg.max_query_error is not None:
        print(
            "       Mode: error-constrained simplification "
            f"(max_query_error={model_cfg.max_query_error}, "
            f"iterations={model_cfg.max_search_iterations}, "
            f"tolerance={model_cfg.error_tolerance}, model={model_variant})"
        )
        _, _, ml_scores = simplify_trajectories(
            points,
            trained_model,
            queries,
            threshold=0.0,
            query_scores=importance,
            model_max_points=model_cfg.model_max_points,
            importance_chunk_size=model_cfg.importance_chunk_size,
            trajectory_boundaries=trajectory_boundaries,
            compression_ratio=None,
            min_points_per_trajectory=model_cfg.min_points_per_trajectory,
            turn_bias_weight=eff_turn_bias,
        )
        best_threshold, achieved_mean_err, achieved_max_err = find_optimal_threshold(
            scores=ml_scores,
            points=points,
            queries=queries,
            max_query_error=model_cfg.max_query_error,
            boundaries=trajectory_boundaries,
            min_points_per_trajectory=model_cfg.min_points_per_trajectory,
            max_search_iterations=model_cfg.max_search_iterations,
            error_tolerance=model_cfg.error_tolerance,
        )
        ml_simplified, retained_mask = apply_threshold_simplification(
            points,
            ml_scores,
            best_threshold,
            trajectory_boundaries,
            model_cfg.min_points_per_trajectory,
        )
        achieved_ratio = compute_compression_ratio(points, ml_simplified)
        print(
            f"       Error-constrained result: "
            f"threshold={best_threshold:.6f}, "
            f"mean_error={achieved_mean_err:.6f}, "
            f"max_error={achieved_max_err:.6f}, "
            f"compression_ratio={achieved_ratio:.4f}"
        )
        return ModelSimplificationResult(
            simplified_points=ml_simplified,
            retained_mask=retained_mask,
            scores=ml_scores,
            label=label,
        )

    if model_cfg.compression_ratio is not None:
        print(
            "       Mode: per-trajectory compression "
            f"(ratio={model_cfg.compression_ratio}, "
            f"min_pts={model_cfg.min_points_per_trajectory}, model={model_variant})"
        )
        ml_simplified, retained_mask, ml_scores = simplify_trajectories(
            points,
            trained_model,
            queries,
            query_scores=importance,
            model_max_points=model_cfg.model_max_points,
            importance_chunk_size=model_cfg.importance_chunk_size,
            trajectory_boundaries=trajectory_boundaries,
            compression_ratio=model_cfg.compression_ratio,
            min_points_per_trajectory=model_cfg.min_points_per_trajectory,
            turn_bias_weight=eff_turn_bias,
        )
        return ModelSimplificationResult(
            simplified_points=ml_simplified,
            retained_mask=retained_mask,
            scores=ml_scores,
            label=label,
        )

    if model_cfg.target_ratio is not None:
        if not (0.0 < model_cfg.target_ratio <= 1.0):
            raise ValueError("target_ratio must be in (0, 1].")

        _, _, ml_scores = simplify_trajectories(
            points,
            trained_model,
            queries,
            threshold=0.0,
            query_scores=importance,
            model_max_points=model_cfg.model_max_points,
            importance_chunk_size=model_cfg.importance_chunk_size,
            trajectory_boundaries=trajectory_boundaries,
            compression_ratio=None,
            turn_bias_weight=eff_turn_bias,
        )

        n_points_total = points.shape[0]
        target_count = max(
            1,
            min(n_points_total, int(round(model_cfg.target_ratio * n_points_total))),
        )
        topk_vals, topk_idx = torch.topk(ml_scores, k=target_count)
        effective_threshold = float(topk_vals.min().item())

        retained_mask = torch.zeros(n_points_total, dtype=torch.bool, device=points.device)
        retained_mask[topk_idx] = True
        ml_simplified = points[retained_mask]

        print(
            f"       target_ratio={model_cfg.target_ratio:.4f} "
            f"-> auto-threshold={effective_threshold:.4f}"
        )
        return ModelSimplificationResult(
            simplified_points=ml_simplified,
            retained_mask=retained_mask,
            scores=ml_scores,
            label=label,
        )

    ml_simplified, retained_mask, ml_scores = simplify_trajectories(
        points,
        trained_model,
        queries,
        threshold=model_cfg.threshold,
        query_scores=importance,
        model_max_points=model_cfg.model_max_points,
        importance_chunk_size=model_cfg.importance_chunk_size,
        trajectory_boundaries=trajectory_boundaries,
        compression_ratio=None,
        min_points_per_trajectory=model_cfg.min_points_per_trajectory,
        turn_bias_weight=eff_turn_bias,
    )
    return ModelSimplificationResult(
        simplified_points=ml_simplified,
        retained_mask=retained_mask,
        scores=ml_scores,
        label=label,
    )


def _build_methods_for_evaluation(
    points: torch.Tensor,
    ml_ratio: float,
    ml_results_by_type: dict[str, ModelSimplificationResult],
    baseline_cfg: BaselineConfig,
) -> dict[str, torch.Tensor]:
    """Build the method-to-points map for evaluation, including optional baselines."""
    methods: dict[str, torch.Tensor] = {
        result.label: result.simplified_points for result in ml_results_by_type.values()
    }

    if baseline_cfg.skip_baselines:
        print("       Skipping baselines (--skip_baselines enabled).")
        return methods

    methods["Random"] = random_sampling(points, ratio=ml_ratio)
    methods["Temporal"] = uniform_temporal_sampling(points, ratio=ml_ratio)

    if points.shape[0] <= baseline_cfg.dp_max_points:
        methods["Douglas-Peucker"] = douglas_peucker(points, epsilon=0.01)
    else:
        print(
            "       Skipping Douglas-Peucker baseline for large dataset "
            f"({points.shape[0]} points > dp_max_points={baseline_cfg.dp_max_points})."
        )

    return methods


def _evaluate_methods(
    points: torch.Tensor,
    queries: torch.Tensor,
    methods: dict[str, torch.Tensor],
) -> dict[str, MethodMetrics]:
    """Evaluate all simplified-method outputs against query metrics."""
    results: dict[str, MethodMetrics] = {}
    for name, simplified in methods.items():
        results[name] = MethodMetrics(
            query_error=query_error(points, simplified, queries),
            compression_ratio=compute_compression_ratio(points, simplified),
            latency_ms=query_latency(simplified, queries) * 1000,
        )
    return results


def _evaluate_typed_methods(
    points: torch.Tensor,
    typed_queries: list[dict],
    methods: dict[str, torch.Tensor],
) -> tuple[dict[str, MethodMetrics], dict[str, dict[str, float]]]:
    """Evaluate simplified methods using typed queries."""
    from src.evaluation.metrics import compute_typed_query_error

    method_metrics: dict[str, MethodMetrics] = {}
    per_type_breakdown: dict[str, dict[str, float]] = {}

    for name, simplified in methods.items():
        mean_err, type_errs = compute_typed_query_error(points, simplified, typed_queries)
        method_metrics[name] = MethodMetrics(
            query_error=mean_err,
            compression_ratio=compute_compression_ratio(points, simplified),
            latency_ms=0.0,  # Execution time is not measured for typed query evaluation
        )
        per_type_breakdown[name] = type_errs

    return method_metrics, per_type_breakdown


def _print_typed_method_comparison_table(
    results: dict[str, MethodMetrics],
    per_type_errors: dict[str, dict[str, float]],
) -> None:
    """Print per-method evaluation table with per-query-type error breakdown."""
    print("\n" + "=" * 67)
    print(f"{'Method':<20} {'Query Error':>12} {'Comp. Ratio':>12}")
    print("-" * 67)
    for name, metrics in results.items():
        err_str = _format_query_error_value(metrics.query_error)
        print(
            f"{name:<20} {err_str:>12} "
            f"{metrics.compression_ratio:>12.4f}"
        )
        type_errs = per_type_errors.get(name, {})
        for qt, te in sorted(type_errs.items()):
            te_str = _format_query_error_value(te)
            print(f"  {'  ' + qt + ' error':<18} {te_str:>12}")
    print("=" * 67)


def _print_method_comparison_table(results: dict[str, MethodMetrics]) -> None:
    """Print per-method evaluation metrics in tabular form."""
    print("\n" + "=" * 67)
    print(f"{'Method':<20} {'Query Error':>12} {'Comp. Ratio':>12} {'Latency (ms)':>14}")
    print("-" * 67)
    for name, metrics in results.items():
        err_str = _format_query_error_value(metrics.query_error)
        print(
            f"{name:<20} {err_str:>12} "
            f"{metrics.compression_ratio:>12.4f} {metrics.latency_ms:>14.4f}"
        )
    print("=" * 67)


def _save_visualizations(
    workload: str,
    trajectories: list[torch.Tensor],
    points: torch.Tensor,
    importance: torch.Tensor,
    retained_mask: torch.Tensor,
    ml_scores: torch.Tensor,
    queries: torch.Tensor | list[dict],
    primary_label: str,
    ml_results_by_type: dict[str, ModelSimplificationResult],
    viz_cfg: VisualizationConfig,
) -> None:
    """Generate and save experiment visualization artifacts."""
    from src.visualization.trajectory_visualizer import (
        plot_queries_on_trajectories,
        plot_typed_queries_on_trajectories,
        plot_trajectories,
    )
    from src.visualization.importance_visualizer import (
        plot_importance,
        plot_simplification_results,
        plot_simplification_time_slices,
        plot_trajectories_with_importance_and_queries,
    )

    temp_dir = tempfile.gettempdir()
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs("results", exist_ok=True)

    trajectories_path = os.path.join(temp_dir, "ais_trajectories.png")
    queries_path = os.path.join(temp_dir, f"ais_queries_{workload}.png")
    importance_path = os.path.join(temp_dir, "ais_importance.png")
    combined_path = os.path.join(temp_dir, "ais_combined.png")
    time_slices_path = "results/simplification_time_slices.png"

    print(f"\n[8/8] Saving visualizations to {temp_dir} and results/ …")

    viz_trajectories = _downsample_trajectories_for_visualization(
        trajectories,
        max_ships=viz_cfg.max_visualization_ships,
        max_points_per_ship=viz_cfg.max_points_per_ship_plot,
    )
    viz_points, viz_importance, viz_retained_mask, viz_scores = _downsample_points_for_visualization(
        points,
        importance,
        retained_mask,
        ml_scores,
        max_points=viz_cfg.max_visualization_points,
    )

    if viz_points.shape[0] < points.shape[0] or len(viz_trajectories) < len(trajectories):
        print(
            "       Visualization downsampling: "
            f"points {viz_points.shape[0]}/{points.shape[0]}, "
            f"ships {len(viz_trajectories)}/{len(trajectories)}."
        )

    viz_trajectories_cpu = [traj.detach().cpu() for traj in viz_trajectories]
    viz_points_cpu = viz_points.detach().cpu()
    viz_importance_cpu = viz_importance.detach().cpu()
    viz_retained_mask_cpu = viz_retained_mask.detach().cpu()
    viz_scores_cpu = viz_scores.detach().cpu()

    typed_queries_list: list[dict] | None = None
    range_queries_tensor: torch.Tensor | None = None
    if isinstance(queries, list):
        typed_queries_list = queries
    else:
        range_queries_tensor = queries.detach().cpu()

    plot_trajectories(
        viz_trajectories_cpu,
        title="AIS Trajectories",
        save_path=trajectories_path,
    )

    if typed_queries_list is not None:
        plot_typed_queries_on_trajectories(
            viz_trajectories_cpu,
            typed_queries_list,
            title=f"AIS Trajectories with {_workload_title(workload)}",
            save_path=queries_path,
        )
    else:
        assert range_queries_tensor is not None
        plot_queries_on_trajectories(
            viz_trajectories_cpu,
            range_queries_tensor,
            title=f"AIS Trajectories with {_workload_title(workload)}",
            save_path=queries_path,
        )

    plot_importance(
        viz_points_cpu,
        viz_importance_cpu,
        title="Ground-Truth Point Importance",
        save_path=importance_path,
    )

    if range_queries_tensor is not None:
        plot_trajectories_with_importance_and_queries(
            viz_trajectories_cpu,
            viz_points_cpu,
            viz_importance_cpu,
            range_queries_tensor,
            title=f"AIS QDS: Importance + Queries ({_workload_title(workload)})",
            save_path=combined_path,
        )
        simplification_queries = range_queries_tensor
    else:
        simplification_queries = torch.zeros(0, 6)

    plot_simplification_results(
        viz_trajectories_cpu,
        viz_points_cpu,
        viz_retained_mask_cpu,
        viz_scores_cpu,
        simplification_queries,
        title=f"AIS Trajectory Simplification Results ({primary_label})",
        save_path="results/simplification_visualization.png",
    )
    plot_simplification_time_slices(
        viz_points_cpu,
        viz_retained_mask_cpu,
        viz_scores_cpu,
        simplification_queries,
        title="AIS Simplification (Time-Sliced Query Context)",
        n_slices=4,
        save_path=time_slices_path,
    )

    if "turn_aware" in ml_results_by_type:
        from src.visualization.importance_visualizer import plot_turn_scores

        turn_path = os.path.join(temp_dir, "ais_turn_scores.png")
        plot_turn_scores(
            viz_points_cpu,
            viz_retained_mask_cpu,
            title="AIS Turn-Score Visualization (Turn-Aware Model)",
            save_path=turn_path,
        )
        print(f"       Saved: {turn_path}")

    print(f"       Saved: {trajectories_path}")
    print(f"       Saved: {queries_path}")
    print(f"       Saved: {importance_path}")
    print(f"       Saved: {combined_path}")
    print("       Saved: results/simplification_visualization.png")
    print(f"       Saved: {time_slices_path}")
    print("\nDone.")
