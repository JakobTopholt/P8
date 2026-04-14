"""End-to-end AIS QDS experiment pipeline. See src/experiments/README.md for full details."""

from __future__ import annotations

import glob
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from src.experiments.experiment_cli import parse_and_validate_experiment_args
from src.experiments.experiment_config import (
    ExperimentConfig,
    MethodMetrics,
    build_experiment_config,
)
from src.experiments.experiment_pipeline_helpers import (
    _print_workload_comparison_table,
    _workload_title,
)
from src.experiments.workload_runner import run_single_workload


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
) -> None:
    """Run the full AIS QDS experiment and print a results table."""
    config = build_experiment_config(**locals())

    if config.query.workload == "all":
        # Run each workload independently and collect results for a combined table
        all_results: dict[str, dict[str, MethodMetrics]] = {}
        for wl in ("uniform", "density", "mixed"):
            print(f"\n{'='*65}")
            print(f"Running workload: {_workload_title(wl)}")
            print(f"{'='*65}")
            wl_config = replace(config, query=replace(config.query, workload=wl))
            wl_results = run_single_workload(wl_config)
            all_results[wl] = wl_results

        # Print combined comparison table
        _print_workload_comparison_table(all_results)
        return

    run_single_workload(config)


def _run_single_workload(config: ExperimentConfig) -> dict[str, MethodMetrics]:
    """Backward-compatible wrapper for the extracted workload runner."""
    return run_single_workload(config)


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
