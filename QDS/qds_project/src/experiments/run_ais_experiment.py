"""End-to-end AIS QDS experiment pipeline. See src/experiments/README.md for full details."""

from __future__ import annotations

import glob
import os
import shutil
import sys
import time
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

import torch

from src.data.ais_loader import generate_synthetic_ais_data, load_ais_csv
from src.data.trajectory_dataset import TrajectoryDataset
from src.queries.query_executor import run_queries
from src.training.importance_labels import compute_importance
from src.training.train_model import train_model
from src.experiments.experiment_cli import parse_and_validate_experiment_args
from src.experiments.experiment_config import (
    ExperimentConfig,
    MethodMetrics,
    ModelSimplificationResult,
    build_experiment_config,
)
from src.experiments.experiment_pipeline_helpers import (
    _build_methods_for_evaluation,
    _clean_csv_output_path,
    _count_removed_overlap_stats,
    _evaluate_methods,
    _evaluate_typed_methods,
    _generate_queries_for_workload,
    _print_method_comparison_table,
    _print_typed_method_comparison_table,
    _print_workload_comparison_table,
    _resolve_effective_max_train_points,
    _resolve_model_variants,
    _save_retained_points_csv,
    _save_visualizations,
    _simplify_with_model,
    _workload_title,
)
from src.evaluation.metrics import (
    compression_ratio as compute_compression_ratio,
)
from src.experiments.experiment_config import TypedQueryWorkload


def run_ais_experiment(
    n_ships: int = 10,
    n_points: int = 100,
    n_queries: int = 100,
    epochs: int = 50,
    threshold: float = 0.5,
    target_ratio: float | None = None,
    compression_ratio: float | None = 0.2,
    min_points_per_trajectory: int = 5,
    max_train_points: int | None = None,
    model_max_points: int | None = 300_000,
    point_batch_size: int = 50_000,
    importance_chunk_size: int = 200_000,
    dp_max_points: int = 200_000,
    skip_baselines: bool = False,
    skip_visualizations: bool = False,
    max_visualization_points: int = 200_000,
    max_visualization_ships: int = 200,
    max_points_per_ship_plot: int = 2_000,
    csv_path: str | None = None,
    save_csv: bool = False,
    workload: str = "density",
    density_ratio: float = 0.7,
    query_spatial_fraction: float = 0.03,
    query_temporal_fraction: float = 0.10,
    query_spatial_lower_quantile: float = 0.01,
    query_spatial_upper_quantile: float = 0.99,
    model_type: str = "baseline",
    turn_bias_weight: float = 0.1,
    turn_score_method: str = "heading",
    max_query_error: float | None = None,
    max_search_iterations: int = 20,
    error_tolerance: float = 1e-3,
) -> None:
    """Run the full AIS QDS experiment and print a results table."""
    config = build_experiment_config(
        n_ships=n_ships,
        n_points=n_points,
        n_queries=n_queries,
        epochs=epochs,
        threshold=threshold,
        target_ratio=target_ratio,
        compression_ratio=compression_ratio,
        min_points_per_trajectory=min_points_per_trajectory,
        max_train_points=max_train_points,
        model_max_points=model_max_points,
        point_batch_size=point_batch_size,
        importance_chunk_size=importance_chunk_size,
        dp_max_points=dp_max_points,
        skip_baselines=skip_baselines,
        skip_visualizations=skip_visualizations,
        max_visualization_points=max_visualization_points,
        max_visualization_ships=max_visualization_ships,
        max_points_per_ship_plot=max_points_per_ship_plot,
        csv_path=csv_path,
        save_csv=save_csv,
        workload=workload,
        density_ratio=density_ratio,
        query_spatial_fraction=query_spatial_fraction,
        query_temporal_fraction=query_temporal_fraction,
        query_spatial_lower_quantile=query_spatial_lower_quantile,
        query_spatial_upper_quantile=query_spatial_upper_quantile,
        model_type=model_type,
        turn_bias_weight=turn_bias_weight,
        turn_score_method=turn_score_method,
        max_query_error=max_query_error,
        max_search_iterations=max_search_iterations,
        error_tolerance=error_tolerance,
    )

    if config.query.workload == "all":
        # Run each workload independently and collect results for a combined table
        all_results: dict[str, dict[str, MethodMetrics]] = {}
        for wl in ("uniform", "density", "mixed", "intersection", "aggregation", "nearest", "multi"):
            print(f"\n{'='*65}")
            print(f"Running workload: {_workload_title(wl)}")
            print(f"{'='*65}")
            wl_config = replace(config, query=replace(config.query, workload=wl))
            wl_results = _run_single_workload(wl_config)
            all_results[wl] = wl_results

        # Print combined comparison table
        _print_workload_comparison_table(all_results)
        return

    _run_single_workload(config)


def _run_single_workload(config: ExperimentConfig) -> dict[str, MethodMetrics]:
    """Run the full AIS QDS pipeline for a single query workload."""
    data_cfg = config.data
    query_cfg = config.query
    model_cfg = config.model
    baseline_cfg = config.baselines
    viz_cfg = config.visualization
    workload = query_cfg.workload

    print("=" * 65)
    print("AIS Query-Driven Simplification (QDS) Experiment")
    print(f"Workload: {_workload_title(workload)}")
    print(f"Turn score method: {model_cfg.turn_score_method}")
    if model_cfg.max_query_error is not None:
        print(
            f"Mode: error-constrained (max_query_error={model_cfg.max_query_error}, "
            f"max_search_iterations={model_cfg.max_search_iterations}, "
            f"error_tolerance={model_cfg.error_tolerance})"
        )
    print("=" * 65)

    # Select compute device (GPU if available, otherwise CPU).
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"       Device: {device}")

    # ------------------------------------------------------------------
    # 1. Load or generate AIS data
    # ------------------------------------------------------------------
    loaded_from_csv = bool(data_cfg.csv_path and os.path.exists(data_cfg.csv_path))

    _t0 = time.time()
    if loaded_from_csv:
        assert data_cfg.csv_path is not None
        print(f"\n[1/8] Loading AIS data from {data_cfg.csv_path} …")
        trajectories = load_ais_csv(data_cfg.csv_path, turn_score_method=model_cfg.turn_score_method)
    else:
        print(
            "\n[1/8] Generating synthetic AIS data "
            f"({data_cfg.n_ships} ships × {data_cfg.n_points_per_ship} pts) …"
        )
        trajectories = generate_synthetic_ais_data(
            n_ships=data_cfg.n_ships,
            n_points_per_ship=data_cfg.n_points_per_ship,
            turn_score_method=model_cfg.turn_score_method,
        )

    dataset = TrajectoryDataset(trajectories)
    points = dataset.get_all_points().to(device)  # [N, 7]
    traj_boundaries = dataset.get_trajectory_boundaries()
    print(f"       Total ships: {len(trajectories)}, Total points: {points.shape[0]}")
    print(f"       Data loading time: {time.time() - _t0:.2f}s")

    # ------------------------------------------------------------------
    # 2. Generate query workload
    # ------------------------------------------------------------------
    _t0 = time.time()
    print(
        f"\n[2/8] Generating {query_cfg.n_queries} queries "
        f"({_workload_title(workload)}) …"
    )
    query_result = _generate_queries_for_workload(trajectories, query_cfg)

    # Unpack: typed workloads return a TypedQueryWorkload; classic ones return
    # a plain Tensor.  Training and importance computation always use range
    # queries (tensor format) for compatibility with the model architecture.
    typed_queries: list[dict] | None = None
    if isinstance(query_result, TypedQueryWorkload):
        queries = query_result.range_queries.to(device)
        typed_queries = query_result.typed_queries
        print(f"       Range query tensor shape (for training): {queries.shape}")
        print(f"       Typed queries for evaluation: {len(typed_queries)}")
        # Count types
        type_counts: dict[str, int] = {}
        for q in typed_queries:
            type_counts[q.get("type", "unknown")] = type_counts.get(q.get("type", "unknown"), 0) + 1
        print(f"       Type breakdown: {type_counts}")
    else:
        queries = query_result.to(device)
        print(f"       Query tensor shape: {queries.shape}")
        lat_span = (queries[:, 1] - queries[:, 0]).mean().item()
        lon_span = (queries[:, 3] - queries[:, 2]).mean().item()
        time_span = (queries[:, 5] - queries[:, 4]).mean().item()
        print(
            "       Avg query spans "
            f"lat={lat_span:.4f}, lon={lon_span:.4f}, time={time_span:.2f}"
        )
    print(f"       Query generation time: {time.time() - _t0:.2f}s")

    # ------------------------------------------------------------------
    # 3. Compute importance labels
    # ------------------------------------------------------------------
    _t0 = time.time()
    print("\n[3/8] Computing ground-truth importance labels …")
    importance = compute_importance(points, queries, chunk_size=model_cfg.importance_chunk_size)
    print(f"       Importance range: [{importance.min():.4f}, {importance.max():.4f}]")
    print(f"       Importance computation time: {time.time() - _t0:.2f}s")

    effective_max_train_points, used_auto_cap = _resolve_effective_max_train_points(
        n_points=points.shape[0],
        model_cfg=model_cfg,
    )
    if used_auto_cap and effective_max_train_points is not None:
        print(
            "       Large dataset detected: using sampled training subset "
            f"{effective_max_train_points}/{points.shape[0]} points."
        )

    # ------------------------------------------------------------------
    # 4. Train model(s)
    # ------------------------------------------------------------------
    _t0 = time.time()
    model_variants = _resolve_model_variants(model_cfg.model_type)

    _model_class_names: dict[str, str] = {
        "baseline": "TrajectoryQDSModel",
        "turn_aware": "TurnAwareQDSModel",
    }

    models: dict[str, object] = {}
    for mt in model_variants:
        model_name = _model_class_names.get(mt, mt)
        print(f"\n[4/8] Training {model_name} ({model_cfg.epochs} epochs, model_type={mt}) …")
        models[mt] = train_model(
            trajectories,
            queries,
            epochs=model_cfg.epochs,
            importance=importance,
            max_points=effective_max_train_points,
            importance_chunk_size=model_cfg.importance_chunk_size,
            point_batch_size=model_cfg.point_batch_size,
            model_type=mt,
        )
    print(f"       Model training time: {time.time() - _t0:.2f}s")

    # ------------------------------------------------------------------
    # 5. Simplify with model(s)
    # ------------------------------------------------------------------
    _t0 = time.time()
    print("\n[5/8] Simplifying trajectories with trained model(s) …")

    ml_results_by_type: dict[str, ModelSimplificationResult] = {}
    for mt, trained_model in models.items():
        run_result = _simplify_with_model(
            points=points,
            queries=queries,
            importance=importance,
            trained_model=trained_model,
            model_variant=mt,
            model_cfg=model_cfg,
            trajectory_boundaries=traj_boundaries,
        )
        ratio_for_model = compute_compression_ratio(points, run_result.simplified_points)
        print(
            f"       {run_result.label} retained "
            f"{run_result.simplified_points.shape[0]}/{points.shape[0]} points "
            f"(ratio={ratio_for_model:.3f})"
        )
        ml_results_by_type[mt] = run_result

    primary_result = ml_results_by_type[model_variants[0]]
    ml_simplified = primary_result.simplified_points
    retained_mask = primary_result.retained_mask
    ml_scores = primary_result.scores
    primary_label = primary_result.label
    ml_ratio = compute_compression_ratio(points, ml_simplified)

    print(f"       Simplification time: {time.time() - _t0:.2f}s")

    # Trajectory retention statistics (Feature 6)
    n_trajectories = len(trajectories)
    n_traj_retained = sum(
        1 for start, end in traj_boundaries if retained_mask[start:end].any().item()
    )
    avg_pts_before = points.shape[0] / max(1, n_trajectories)
    avg_pts_after = ml_simplified.shape[0] / max(1, n_trajectories)
    print(f"       Trajectories retained: {n_traj_retained}/{n_trajectories}")
    print(f"       Avg points per trajectory before: {avg_pts_before:.1f}")
    print(f"       Avg points per trajectory after:  {avg_pts_after:.1f}")

    if loaded_from_csv and data_cfg.csv_path is not None:
        if data_cfg.save_csv:
            clean_csv_path = _clean_csv_output_path(data_cfg.csv_path)
            _save_retained_points_csv(ml_simplified, clean_csv_path)
            print(f"       Saved retained points CSV: {clean_csv_path}")
        else:
            print("       Skipped saving cleaned file (set --save_csv to enable).")

    removed_mask = ~retained_mask
    removed_in_spatial, removed_in_spatiotemporal = _count_removed_overlap_stats(
        points,
        removed_mask,
        queries,
        chunk_size=model_cfg.importance_chunk_size,
    )

    print(
        "       Removed points in spatial rectangles: "
        f"{removed_in_spatial}/{int(removed_mask.sum().item())}"
    )
    print(
        "       Removed points in full spatiotemporal queries: "
        f"{removed_in_spatiotemporal}/{int(removed_mask.sum().item())}"
    )

    if removed_in_spatial > 0 and removed_in_spatiotemporal == 0:
        print(
            "       Note: query rectangles in the plot are spatial only; "
            "removed points can be outside query time windows."
        )

    full_query_results = run_queries(points, queries)
    ml_query_results = run_queries(ml_simplified, queries)
    abs_diff = (full_query_results - ml_query_results).abs()
    rel_diff = abs_diff / full_query_results.abs().clamp(min=1e-8)

    # Raw numerical differences can be non-zero due to floating-point reduction
    # order even when practical query outcomes are unchanged.
    changed_queries_any = int((abs_diff > 1e-12).sum().item())
    changed_queries_meaningful = int((rel_diff > 1e-4).sum().item())

    print(
        "       Queries changed by ML QDS "
        f"(any delta > 1e-12): {changed_queries_any}/{queries.shape[0]}"
    )
    print(
        "       Queries with meaningful relative change "
        f"(> 1e-4): {changed_queries_meaningful}/{queries.shape[0]}"
    )

    q = torch.tensor([0.50, 0.90, 0.95, 0.99], dtype=ml_scores.dtype, device=ml_scores.device)
    q_vals = torch.quantile(ml_scores, q).tolist()
    print(
        "       Score quantiles "
        f"p50={q_vals[0]:.4f}, p90={q_vals[1]:.4f}, p95={q_vals[2]:.4f}, p99={q_vals[3]:.4f}"
    )

    if ml_simplified.shape[0] <= max(3, int(0.01 * points.shape[0])):
        print(
            "       Warning: very few points retained. "
            "Try a lower threshold (e.g. near p90/p95 score quantiles)."
        )

    # ------------------------------------------------------------------
    # 6. Baselines at the same compression ratio
    # ------------------------------------------------------------------
    print(f"\n[6/8] Running baselines at ratio ≈ {ml_ratio:.3f} …")
    methods = _build_methods_for_evaluation(
        points=points,
        ml_ratio=ml_ratio,
        ml_results_by_type=ml_results_by_type,
        baseline_cfg=baseline_cfg,
    )

    # ------------------------------------------------------------------
    # 7. Evaluate all methods
    # ------------------------------------------------------------------
    _t0 = time.time()
    print("\n[7/8] Evaluating all methods …")
    if typed_queries is not None:
        # Typed workload: evaluate using typed query error and report per-type breakdown
        results, per_type_errors = _evaluate_typed_methods(points, typed_queries, methods)
        print(f"       Evaluation time: {time.time() - _t0:.2f}s")
        _print_typed_method_comparison_table(results, per_type_errors)
    else:
        results = _evaluate_methods(points, queries, methods)
        print(f"       Evaluation time: {time.time() - _t0:.2f}s")
        _print_method_comparison_table(results)

    # ------------------------------------------------------------------
    # 8. Visualizations
    # ------------------------------------------------------------------
    if viz_cfg.skip_visualizations:
        print("\n[8/8] Skipping visualizations (--skip_visualizations enabled).")
        print("\nDone.")
        return results

    # For visualization pass typed_queries (list) or range queries (tensor)
    viz_queries: torch.Tensor | list[dict] = typed_queries if typed_queries is not None else queries

    _save_visualizations(
        workload=workload,
        trajectories=trajectories,
        points=points,
        importance=importance,
        retained_mask=retained_mask,
        ml_scores=ml_scores,
        queries=viz_queries,
        primary_label=primary_label,
        ml_results_by_type=ml_results_by_type,
        viz_cfg=viz_cfg,
    )

    return results

# Default project dir derived from code location: QDS/qds_project/src/experiments/ -> project root
_CODE_PROJECT_DIR = str(Path(__file__).resolve().parent.parent.parent.parent.parent)
_DEFAULT_DATE_TAG = "2026-02-05"


def _resolve_project_dir() -> str:
    return os.environ.get("AIS_PROJECT_DIR", _CODE_PROJECT_DIR)


def _resolve_date_tag() -> str:
    return os.environ.get("AIS_DATE_TAG", _DEFAULT_DATE_TAG)


def _find_cleaned_csv() -> str | None:
    """Find the cleaned CSV from env vars or standard project locations."""
    # Explicit path from environment
    csv_env = os.environ.get("AIS_CSV_PATH")
    if csv_env and os.path.isfile(csv_env):
        return csv_env

    project_dir = _resolve_project_dir()
    date_tag = _resolve_date_tag()
    aisdata = os.path.join(project_dir, "AISDATA")

    # Try Spark output directory
    spark_dir = os.path.join(aisdata, f"aisdk-{date_tag}.cleaned.csv")
    part_files = sorted(glob.glob(os.path.join(spark_dir, "part-*.csv")))
    if part_files:
        return part_files[0]

    # Fallback to preprocessed folder
    preprocessed = os.path.join(aisdata, "preprocessed_AIS_files", f"preprocessed_{date_tag}.csv")
    if os.path.isfile(preprocessed):
        return preprocessed

    return None


def _copy_ml_output(csv_path: str) -> None:
    """Copy ML output CSV to ML_processed_AIS_files."""
    project_dir = _resolve_project_dir()
    date_tag = _resolve_date_tag()

    ml_output = os.path.join(
        os.path.dirname(csv_path),
        f"MLClean-{os.path.basename(csv_path)}",
    )
    if not os.path.isfile(ml_output):
        print(f"WARNING: ML output not found at {ml_output}", file=sys.stderr)
        return

    ml_dir = Path(project_dir) / "AISDATA" / "ML_processed_AIS_files"
    ml_dir.mkdir(parents=True, exist_ok=True)
    dest = ml_dir / f"ML_{date_tag}.csv"
    shutil.copy2(ml_output, dest)
    print(f"[ML] Copied to: {dest}")


def main() -> None:
    """Command-line entry point for the AIS QDS experiment."""
    args = parse_and_validate_experiment_args()

    # Auto-discover CSV if not provided via CLI
    if args.get("csv_path") is None:
        found = _find_cleaned_csv()
        if found:
            args["csv_path"] = found
            print(f"[ML] Auto-discovered input: {found}")

    run_ais_experiment(**args)

    # Copy output to ML_processed_AIS_files
    if args.get("csv_path"):
        _copy_ml_output(args["csv_path"])


if __name__ == "__main__":
    main()
