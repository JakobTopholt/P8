"""CLI argument parsing and validation for the AIS experiment."""

from __future__ import annotations

import argparse


def _validate_range(
    parser: argparse.ArgumentParser,
    *,
    name: str,
    value: int | float | None,
    minimum: int | float | None = None,
    maximum: int | float | None = None,
    min_inclusive: bool = True,
    max_inclusive: bool = True,
    allow_none: bool = False,
) -> None:
    """Validate numeric CLI arguments with unified error messaging."""
    if value is None:
        if allow_none:
            return
        parser.error(f"{name} is required.")

    assert value is not None
    if minimum is not None:
        if min_inclusive and value < minimum:
            parser.error(f"{name} must be >= {minimum} (got {value}).")
        if not min_inclusive and value <= minimum:
            parser.error(f"{name} must be > {minimum} (got {value}).")

    if maximum is not None:
        if max_inclusive and value > maximum:
            parser.error(f"{name} must be <= {maximum} (got {value}).")
        if not max_inclusive and value >= maximum:
            parser.error(f"{name} must be < {maximum} (got {value}).")


def _normalize_compression_ratio(compression_ratio: float) -> float | None:
    """Map CLI compression mode sentinel to internal representation."""
    return compression_ratio if compression_ratio > 0.0 else None


def _validate_cli_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate parsed CLI arguments and cross-argument constraints."""
    for flag_name in (
        "--n_ships",
        "--n_points",
        "--n_queries",
        "--epochs",
        "--min_points_per_trajectory",
        "--point_batch_size",
        "--importance_chunk_size",
        "--dp_max_points",
        "--max_visualization_points",
        "--max_visualization_ships",
        "--max_points_per_ship_plot",
    ):
        attr_name = flag_name[2:]
        _validate_range(
            parser,
            name=flag_name,
            value=getattr(args, attr_name),
            minimum=1,
        )

    _validate_range(
        parser,
        name="--max_train_points",
        value=args.max_train_points,
        minimum=1,
        allow_none=True,
    )
    _validate_range(
        parser,
        name="--model_max_points",
        value=args.model_max_points,
        minimum=1,
        allow_none=True,
    )

    _validate_range(
        parser,
        name="--compression_ratio",
        value=args.compression_ratio,
        minimum=0.0,
        maximum=1.0,
    )
    _validate_range(
        parser,
        name="--target_ratio",
        value=args.target_ratio,
        minimum=0.0,
        maximum=1.0,
        min_inclusive=False,
        allow_none=True,
    )
    _validate_range(
        parser,
        name="--density_ratio",
        value=args.density_ratio,
        minimum=0.0,
        maximum=1.0,
    )
    _validate_range(
        parser,
        name="--query_spatial_fraction",
        value=args.query_spatial_fraction,
        minimum=0.0,
        maximum=1.0,
        min_inclusive=False,
    )
    _validate_range(
        parser,
        name="--query_temporal_fraction",
        value=args.query_temporal_fraction,
        minimum=0.0,
        maximum=1.0,
        min_inclusive=False,
    )
    _validate_range(
        parser,
        name="--query_spatial_lower_quantile",
        value=args.query_spatial_lower_quantile,
        minimum=0.0,
        maximum=1.0,
    )
    _validate_range(
        parser,
        name="--query_spatial_upper_quantile",
        value=args.query_spatial_upper_quantile,
        minimum=0.0,
        maximum=1.0,
    )

    if args.query_spatial_lower_quantile >= args.query_spatial_upper_quantile:
        parser.error(
            "--query_spatial_lower_quantile must be < "
            "--query_spatial_upper_quantile."
        )

    if args.save_csv and not args.csv_path:
        parser.error("--save_csv requires --csv_path.")

    if args.target_ratio is not None and args.compression_ratio > 0.0:
        parser.error(
            "--target_ratio is only used in global-threshold mode. "
            "Set --compression_ratio 0 to enable it."
        )

    if args.max_query_error is not None and args.max_query_error <= 0.0:
        parser.error("--max_query_error must be > 0.")

    if args.max_search_iterations < 1:
        parser.error("--max_search_iterations must be >= 1.")

    if args.error_tolerance < 0.0:
        parser.error("--error_tolerance must be >= 0.")


def _build_run_kwargs(
    args: argparse.Namespace,
    parsed_compression_ratio: float | None,
) -> dict[str, object]:
    """Build keyword arguments for run_ais_experiment from parsed CLI args."""
    return {
        "n_ships": args.n_ships,
        "n_points": args.n_points,
        "n_queries": args.n_queries,
        "epochs": args.epochs,
        "threshold": args.threshold,
        "target_ratio": args.target_ratio,
        "compression_ratio": parsed_compression_ratio,
        "min_points_per_trajectory": args.min_points_per_trajectory,
        "max_train_points": args.max_train_points,
        "model_max_points": args.model_max_points,
        "point_batch_size": args.point_batch_size,
        "importance_chunk_size": args.importance_chunk_size,
        "dp_max_points": args.dp_max_points,
        "skip_baselines": args.skip_baselines,
        "skip_visualizations": args.skip_visualizations,
        "max_visualization_points": args.max_visualization_points,
        "max_visualization_ships": args.max_visualization_ships,
        "max_points_per_ship_plot": args.max_points_per_ship_plot,
        "csv_path": args.csv_path,
        "save_csv": args.save_csv,
        "workload": args.workload,
        "density_ratio": args.density_ratio,
        "query_spatial_fraction": args.query_spatial_fraction,
        "query_temporal_fraction": args.query_temporal_fraction,
        "query_spatial_lower_quantile": args.query_spatial_lower_quantile,
        "query_spatial_upper_quantile": args.query_spatial_upper_quantile,
        "model_type": args.model_type,
        "turn_bias_weight": args.turn_bias_weight,
        "turn_score_method": args.turn_score_method,
        "max_query_error": args.max_query_error,
        "max_search_iterations": args.max_search_iterations,
        "error_tolerance": args.error_tolerance,
    }


def _add_data_arguments(parser) -> None:
    """Register data-loading and data-generation CLI arguments."""
    parser.add_argument("--n_ships", type=int, default=10, help="Number of ships")
    parser.add_argument("--n_points", type=int, default=100, help="Points per ship")
    parser.add_argument("--csv_path", type=str, default=None, help="Path to AIS CSV file")
    parser.add_argument(
        "--save_csv",
        action="store_true",
        help="Save cleaned retained points CSV when loading data from --csv_path",
    )


def _add_query_arguments(parser) -> None:
    """Register query-workload CLI arguments."""
    parser.add_argument("--n_queries", type=int, default=100, help="Number of queries")
    parser.add_argument(
        "--workload",
        type=str,
        default="density",
        choices=["uniform", "density", "mixed", "intersection", "aggregation", "nearest", "multi", "all"],
        help=(
            "Query workload type: 'uniform' (bounding-box sampling), "
            "'density' (AIS-point-anchored sampling), "
            "'mixed' (blend of uniform+density), "
            "'intersection' (trajectory-intersection queries), "
            "'aggregation' (point-count queries), "
            "'nearest' (nearest-neighbour distance queries), "
            "'multi' (equal blend of all four typed query types), "
            "or 'all' (run all workloads and print a comparison table)."
        ),
    )
    parser.add_argument(
        "--density_ratio",
        type=float,
        default=0.7,
        help=(
            "Fraction of density-biased queries in a 'mixed' workload "
            "(ignored for other workload types). Must be in [0, 1]."
        ),
    )
    parser.add_argument(
        "--query_spatial_fraction",
        type=float,
        default=0.03,
        help=(
            "Maximum spatial query width as a fraction of effective lat/lon "
            "range. Lower values produce tighter query boxes."
        ),
    )
    parser.add_argument(
        "--query_temporal_fraction",
        type=float,
        default=0.10,
        help=(
            "Maximum temporal query width as a fraction of time range. "
            "Lower values produce shorter query windows."
        ),
    )
    parser.add_argument(
        "--query_spatial_lower_quantile",
        type=float,
        default=0.01,
        help=(
            "Lower quantile for robust spatial bounds used by uniform query "
            "placement (and uniform part of mixed workload)."
        ),
    )
    parser.add_argument(
        "--query_spatial_upper_quantile",
        type=float,
        default=0.99,
        help=(
            "Upper quantile for robust spatial bounds used by uniform query "
            "placement (and uniform part of mixed workload)."
        ),
    )


def _add_model_arguments(parser) -> None:
    """Register model-training and simplification CLI arguments."""
    parser.add_argument("--epochs", type=int, default=50, help="Training epochs")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Compression threshold (global mode only)",
    )
    parser.add_argument(
        "--target_ratio",
        type=float,
        default=None,
        help="Target retained ratio in (0, 1]; overrides --threshold (global mode only)",
    )
    parser.add_argument(
        "--compression_ratio",
        type=float,
        default=0.2,
        help=(
            "Per-trajectory compression fraction in (0, 1] (default: 0.2). "
            "Each trajectory keeps max(min_points_per_trajectory, "
            "int(compression_ratio * traj_len)) points. "
            "Pass 0 to disable per-trajectory mode and use global --threshold instead."
        ),
    )
    parser.add_argument(
        "--min_points_per_trajectory",
        type=int,
        default=5,
        help="Minimum number of points to retain per trajectory (default: 5).",
    )
    parser.add_argument(
        "--max_train_points",
        type=int,
        default=None,
        help="Optional cap on number of points used for model training",
    )
    parser.add_argument(
        "--model_max_points",
        type=int,
        default=300000,
        help="Optional cap for full-set model inference during simplification",
    )
    parser.add_argument(
        "--point_batch_size",
        type=int,
        default=50000,
        help="Mini-batch size over points during training",
    )
    parser.add_argument(
        "--importance_chunk_size",
        type=int,
        default=200000,
        help="Chunk size for large-scale importance computation",
    )
    parser.add_argument(
        "--model_type",
        type=str,
        default="baseline",
        choices=["baseline", "turn_aware", "all"],
        help=(
            "Model variant to use: 'baseline' (TrajectoryQDSModel, 7 features), "
            "'turn_aware' (TurnAwareQDSModel, 8 features with turn bias), "
            "or 'all' (train and compare both models). Default: 'baseline'."
        ),
    )
    parser.add_argument(
        "--turn_bias_weight",
        type=float,
        default=0.1,
        help=(
            "Additive weight for turn-score bias applied during simplification "
            "when model_type is 'turn_aware'. Default: 0.1."
        ),
    )
    parser.add_argument(
        "--turn_score_method",
        type=str,
        default="heading",
        choices=["heading", "geometry"],
        help=(
            "Method used to compute turn_score: 'heading' (default, wrapped "
            "COG/heading deltas) or 'geometry' (turn angle from lat/lon vectors)."
        ),
    )
    parser.add_argument(
        "--max_query_error",
        type=float,
        default=None,
        help=(
            "Maximum acceptable mean relative query error (ε). When provided, "
            "binary search is used to find the highest importance threshold "
            "that keeps mean query error ≤ ε. Overrides per-trajectory "
            "--compression_ratio and fixed --threshold modes."
        ),
    )
    parser.add_argument(
        "--max_search_iterations",
        type=int,
        default=20,
        help=(
            "Number of binary-search iterations when --max_query_error is set. "
            "Default: 20."
        ),
    )
    parser.add_argument(
        "--error_tolerance",
        type=float,
        default=1e-3,
        help=(
            "Small slack added to --max_query_error when deciding whether a "
            "threshold is acceptable during binary search. Default: 1e-3."
        ),
    )


def _add_runtime_arguments(parser) -> None:
    """Register evaluation and visualization CLI arguments."""
    parser.add_argument(
        "--dp_max_points",
        type=int,
        default=200000,
        help="Maximum points for Douglas-Peucker baseline",
    )
    parser.add_argument(
        "--skip_baselines",
        action="store_true",
        help="Skip baseline generation/evaluation",
    )
    parser.add_argument(
        "--skip_visualizations",
        action="store_true",
        help="Skip all visualization generation",
    )
    parser.add_argument(
        "--max_visualization_points",
        type=int,
        default=200000,
        help="Maximum points used in visualization scatter plots",
    )
    parser.add_argument(
        "--max_visualization_ships",
        type=int,
        default=200,
        help="Maximum trajectories used in visualization line plots",
    )
    parser.add_argument(
        "--max_points_per_ship_plot",
        type=int,
        default=2000,
        help="Maximum points per trajectory line in visualization",
    )


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser for the experiment entry point."""
    parser = argparse.ArgumentParser(
        description="Run the AIS Query-Driven Simplification experiment"
    )
    _add_data_arguments(parser.add_argument_group("Data"))
    _add_query_arguments(parser.add_argument_group("Query Workload"))
    _add_model_arguments(parser.add_argument_group("Model and Simplification"))
    _add_runtime_arguments(parser.add_argument_group("Evaluation and Visualization"))
    return parser


def parse_and_validate_experiment_args(
    argv: list[str] | None = None,
) -> dict[str, object]:
    """Parse CLI arguments, validate them, and return run kwargs."""
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_cli_args(parser, args)
    parsed_compression_ratio = _normalize_compression_ratio(args.compression_ratio)
    return _build_run_kwargs(args, parsed_compression_ratio)
