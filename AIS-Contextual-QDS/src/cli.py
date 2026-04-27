"""Command line interface for AIS-Contextual-QDS infrastructure."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Sequence

from .config import load_config, read_database_url
from .db import get_connection
from .logging_utils import configure_logging
from .paths import resolve_project_path
from .postgres_tuning import apply_session_profile, apply_system_profile
from .pipelines import (
    benchmarks,
    bootstrap,
    context_loader,
    diagnostics,
    features,
    doctor,
    labels,
    qgis_export,
    reports,
    status,
    subsets,
    trajectories,
    visual_inspection,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_CONFIG_PATH = "configs/iteration1_10days.example.yaml"


def _parse_csv_list(raw_value: str | None) -> list[str] | None:
    if raw_value is None:
        return None
    values = [item.strip() for item in raw_value.split(",")]
    filtered = [item for item in values if item]
    return filtered or None


def _parse_csv_floats(raw_value: str | None) -> list[float] | None:
    values = _parse_csv_list(raw_value)
    if values is None:
        return None
    return [float(value) for value in values]


def _parse_csv_ints(raw_value: str | None) -> list[int] | None:
    values = _parse_csv_list(raw_value)
    if values is None:
        return None
    return [int(value) for value in values]


def _add_run_selector_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Optional simplification run_id to compare against raw trajectories.",
    )
    parser.add_argument(
        "--run-tag",
        default=None,
        help="Optional run_tag filter for selecting a simplification run.",
    )
    parser.add_argument(
        "--method",
        default=None,
        help="Optional method filter for selecting a simplification run, e.g. uniform or dp.",
    )
    parser.add_argument(
        "--budget",
        type=float,
        default=None,
        help="Optional budget filter for selecting a simplification run, e.g. 0.10.",
    )


def _add_subset_sampling_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--split",
        choices=["all", "dev", "eval"],
        default=None,
        help="Subset split to sample when --trajectory-ids is not provided. Defaults to the run split or 'dev'.",
    )
    parser.add_argument(
        "--subset-name",
        default=None,
        help="Subset name override.",
    )
    parser.add_argument(
        "--trajectory-ids",
        default=None,
        help="Comma-separated trajectory IDs to inspect.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="Maximum number of trajectories to export.",
    )


def _add_evaluation_mode_arg(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument(
        "--evaluation-mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help=help_text,
    )


def _add_truth_label_mode_arg(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument(
        "--truth-label-mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help=help_text,
    )


def _run_prepare_data(
    conn,
    config,
    *,
    skip_bootstrap: bool,
    label_mode: str | None,
) -> dict[str, object]:
    if not skip_bootstrap:
        bootstrap.run(conn, config)
        conn.commit()
    trajectories.run(conn, config, truncate=True)
    conn.commit()
    labels.run(conn, config, truncate=True, mode=label_mode)
    conn.commit()
    subsets.run(conn, config, truncate=True)
    conn.commit()
    return status.run(conn, config)


def build_parser() -> argparse.ArgumentParser:
    """Build top-level argument parser."""
    parser = argparse.ArgumentParser(
        description="AIS contextual query-driven simplification workflow for the active Great Belt iteration."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to YAML config file (defaults to the current 10-day iteration config).",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (DEBUG, INFO, WARNING, ERROR).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Create schema and base tables.")

    build_parser = subparsers.add_parser(
        "build-trajectories",
        help="Build raw trajectories from cleaned AIS points.",
    )
    build_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not clear existing trajectory tables before inserting.",
    )

    label_parser = subparsers.add_parser(
        "compute-labels",
        help="Compute zone-entry and corridor-membership labels.",
    )
    label_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not clear existing label table before inserting.",
    )
    label_parser.add_argument(
        "--mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help="Override label semantics mode (default from config performance.label_mode).",
    )

    subset_parser = subparsers.add_parser(
        "create-dev-subset",
        aliases=["subset"],
        help="Create deterministic dev/eval subset assignment.",
    )
    subset_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not delete existing subset rows with the same subset_name.",
    )

    features_parser = subparsers.add_parser(
        "compute-features",
        help="Compute reusable per-point context and local-shape features.",
    )
    features_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not truncate existing point-feature rows before computing.",
    )

    hardcase_parser = subparsers.add_parser(
        "create-hardcase-subset",
        help="Create a deterministic query-positive/negative hard-case dev/eval subset.",
    )
    hardcase_parser.add_argument(
        "--subset-name",
        default=None,
        help="Subset name to create. Defaults to '<configured subset>_hardcase'.",
    )
    hardcase_parser.add_argument(
        "--mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help="Label mode used to balance hard cases.",
    )
    hardcase_parser.add_argument(
        "--dev-size",
        type=int,
        default=None,
        help="Dev split size override.",
    )
    hardcase_parser.add_argument(
        "--eval-size",
        type=int,
        default=None,
        help="Eval split size override.",
    )
    hardcase_parser.add_argument(
        "--min-zone-positives-per-split",
        type=int,
        default=20,
        help="Target minimum positives per zone in each split, limited by availability.",
    )
    hardcase_parser.add_argument(
        "--min-corridor-positives-per-split",
        type=int,
        default=60,
        help="Target minimum corridor-positive trajectories in each split.",
    )
    hardcase_parser.add_argument(
        "--positive-fraction",
        type=float,
        default=0.50,
        help="Approximate query-positive fraction per split after required positives are seeded.",
    )
    hardcase_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not delete existing rows with the same hard-case subset name first.",
    )

    context_parser = subparsers.add_parser(
        "load-context",
        help="Load study region, zones, and corridor geometry files into PostGIS.",
    )
    context_parser.add_argument(
        "--study-region-file",
        required=True,
        help="Path to study-region file (.geojson/.json/.shp).",
    )
    context_parser.add_argument(
        "--zones-file",
        required=True,
        help="Path to zones file (.geojson/.json/.shp).",
    )
    context_parser.add_argument(
        "--corridor-file",
        required=True,
        help="Path to corridor file (.geojson/.json/.shp).",
    )
    context_parser.add_argument(
        "--zones-name-property",
        default="name",
        help="Property used to match zones to config.context.zone_names (default: name).",
    )
    context_parser.add_argument(
        "--corridor-name-property",
        default="name",
        help="Property used to match corridor to config.context.corridor_name (default: name).",
    )
    context_parser.add_argument(
        "--corridor-buffer-meters",
        type=float,
        default=None,
        help="Required when corridor input geometry is line-based; buffers to polygon in meters.",
    )
    context_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not truncate existing context tables before loading.",
    )

    subparsers.add_parser("status", help="Show table counts for current schema.")
    subparsers.add_parser(
        "doctor",
        aliases=["preflight"],
        help="Run environment and dataset preflight checks before heavy pipeline work.",
    )

    balance_parser = subparsers.add_parser(
        "label-balance",
        help="Report label balance overall and for a dev/eval subset.",
    )
    balance_parser.add_argument(
        "--mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help="Label mode to inspect.",
    )
    balance_parser.add_argument(
        "--subset-name",
        default=None,
        help="Subset name to inspect. Defaults to config subsets.subset_name.",
    )
    balance_parser.add_argument(
        "--min-zone-positives",
        type=int,
        default=20,
        help="Warning threshold for zone-positive trajectories.",
    )
    balance_parser.add_argument(
        "--min-corridor-positives",
        type=int,
        default=20,
        help="Warning threshold for corridor-positive trajectories.",
    )

    compare_parser = subparsers.add_parser(
        "compare-label-modes",
        aliases=["audit-label-modes"],
        help="Compare stored labels from two semantics modes, e.g. optimized vs segment_exact.",
    )
    compare_parser.add_argument(
        "--base-mode",
        choices=["optimized", "segment_exact"],
        default="optimized",
        help="Baseline label mode.",
    )
    compare_parser.add_argument(
        "--candidate-mode",
        choices=["optimized", "segment_exact"],
        default="segment_exact",
        help="Candidate/audit label mode.",
    )
    compare_parser.add_argument(
        "--subset-name",
        default=None,
        help="Optional subset name to restrict comparison.",
    )
    compare_parser.add_argument(
        "--split",
        choices=["dev", "eval"],
        default=None,
        help="Optional subset split to restrict comparison.",
    )

    sprint1_parser = subparsers.add_parser(
        "prepare-data",
        aliases=["sprint1"],
        help="Build the working dataset: bootstrap, trajectories, labels, subset, and status.",
    )
    sprint1_parser.add_argument(
        "--skip-bootstrap",
        action="store_true",
        help="Skip schema bootstrap step.",
    )
    sprint1_parser.add_argument(
        "--label-mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help="Override label semantics mode for the prepare-data label step.",
    )

    baseline_parser = subparsers.add_parser(
        "benchmark",
        aliases=["run-baselines"],
        help="Run simplification baselines across retained-point budgets.",
    )
    baseline_parser.add_argument(
        "--methods",
        default=None,
        help="Comma-separated methods override, e.g. 'uniform,dp'.",
    )
    baseline_parser.add_argument(
        "--budgets",
        default=None,
        help="Comma-separated retained-point ratios override, e.g. '0.1,0.2,0.3'.",
    )
    baseline_parser.add_argument(
        "--run-tag",
        default=None,
        help="Optional run tag to group baseline runs.",
    )
    baseline_parser.add_argument(
        "--split",
        choices=["all", "dev", "eval"],
        default=None,
        help="Trajectory split to run (default from config baselines.default_split).",
    )
    baseline_parser.add_argument(
        "--subset-name",
        default=None,
        help="Subset name override (default from config subsets.subset_name).",
    )
    baseline_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing run rows with same run_tag/method/budget.",
    )
    baseline_parser.add_argument(
        "--no-export-summary",
        action="store_true",
        help="Skip CSV/JSON/Markdown/SVG summary export after run completion.",
    )
    baseline_parser.add_argument(
        "--evaluation-mode",
        choices=["optimized", "segment_exact"],
        default=None,
        help="Override evaluation semantics mode (default from config performance.evaluation_mode).",
    )
    _add_truth_label_mode_arg(
        baseline_parser,
        help_text="Select which stored truth-label mode to score against. Defaults to the evaluation mode.",
    )

    summary_parser = subparsers.add_parser(
        "summarize-baselines",
        help="Export summary table files and F1 SVG plot from benchmark metrics.",
    )
    summary_parser.add_argument(
        "--run-tag",
        default=None,
        help="Run tag to summarize. Defaults to latest run_tag.",
    )
    summary_parser.add_argument(
        "--methods",
        default=None,
        help="Optional comma-separated method filter, e.g. 'uniform,dp'.",
    )

    inspect_parser = subparsers.add_parser(
        "inspect-html",
        aliases=["export-visual-inspection"],
        help="Export a self-contained HTML inspection report for raw/context/simplified trajectories.",
    )
    inspect_parser.add_argument(
        "--output",
        default=None,
        help="Output HTML path. Defaults to results/figures/inspection_*.html.",
    )
    _add_run_selector_args(inspect_parser)
    _add_subset_sampling_args(inspect_parser)
    inspect_parser.add_argument(
        "--max-points-per-line",
        type=int,
        default=1500,
        help="Maximum points rendered per raw/simplified SVG line for readability.",
    )
    _add_evaluation_mode_arg(
        inspect_parser,
        help_text="Override prediction semantics mode for inspection overlays.",
    )
    _add_truth_label_mode_arg(
        inspect_parser,
        help_text="Select which stored truth-label mode to display and compare against.",
    )

    qgis_parser = subparsers.add_parser(
        "inspect-qgis",
        aliases=["export-qgis-inspection"],
        help="Export GeoJSON layers and a QGIS project for trajectory/context inspection.",
    )
    qgis_parser.add_argument(
        "--output-dir",
        default=None,
        help="Output folder. Defaults to results/figures/qgis_inspection_*.",
    )
    _add_run_selector_args(qgis_parser)
    _add_subset_sampling_args(qgis_parser)
    qgis_parser.add_argument(
        "--no-points",
        action="store_true",
        help="Skip raw/simplified point layers and export trajectory lines only.",
    )
    _add_evaluation_mode_arg(
        qgis_parser,
        help_text="Override prediction semantics mode for exported comparison layers.",
    )
    _add_truth_label_mode_arg(
        qgis_parser,
        help_text="Select which stored truth-label mode to export as truth.",
    )

    tune_parser = subparsers.add_parser(
        "tune-postgres",
        help="Apply a system-wide PostgreSQL tuning profile with ALTER SYSTEM.",
    )
    tune_parser.add_argument(
        "--profile",
        choices=["laptop_safe"],
        default="laptop_safe",
        help="System tuning profile to apply.",
    )
    tune_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the changes that would be requested without writing them.",
    )

    return parser


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = resolve_project_path(Path(args.config))
    config = load_config(config_path)

    configure_logging(args.log_level or config.paths.log_level)
    LOGGER.info("Using config: %s", config_path)

    database_url = read_database_url(config)

    with get_connection(database_url) as conn:
        session_profile = apply_session_profile(conn, config.performance.session_profile)
        LOGGER.info("Applied PostgreSQL session profile: %s", session_profile)

        if args.command == "tune-postgres":
            _print_json(apply_system_profile(conn, args.profile, dry_run=args.dry_run))
            return 0

        bootstrap.ensure_schema_compatibility(conn, config.database.schema)

        if args.command == "bootstrap":
            bootstrap.run(conn, config)
            return 0

        if args.command == "build-trajectories":
            summary = trajectories.run(conn, config, truncate=not args.append)
            _print_json(summary)
            return 0

        if args.command == "compute-labels":
            summary = labels.run(conn, config, truncate=not args.append, mode=args.mode)
            _print_json(summary)
            return 0

        if args.command in {"create-dev-subset", "subset"}:
            summary = subsets.run(conn, config, truncate=not args.append)
            _print_json(summary)
            return 0

        if args.command == "compute-features":
            summary = features.run(conn, config, truncate=not args.append)
            _print_json(summary)
            return 0

        if args.command == "create-hardcase-subset":
            summary = diagnostics.create_hardcase_subset(
                conn,
                config,
                subset_name=args.subset_name,
                label_mode=args.mode,
                dev_size=args.dev_size,
                eval_size=args.eval_size,
                min_zone_positives_per_split=args.min_zone_positives_per_split,
                min_corridor_positives_per_split=args.min_corridor_positives_per_split,
                positive_fraction=args.positive_fraction,
                truncate=not args.append,
            )
            _print_json(summary)
            return 0

        if args.command == "load-context":
            summary = context_loader.run(
                conn,
                config,
                study_region_path=resolve_project_path(Path(args.study_region_file)),
                zones_path=resolve_project_path(Path(args.zones_file)),
                corridor_path=resolve_project_path(Path(args.corridor_file)),
                zones_name_property=args.zones_name_property,
                corridor_name_property=args.corridor_name_property,
                append=args.append,
                corridor_buffer_meters=args.corridor_buffer_meters,
            )
            _print_json(summary)
            return 0

        if args.command == "status":
            _print_json(status.run(conn, config))
            return 0

        if args.command in {"doctor", "preflight"}:
            _print_json(doctor.run(conn, config))
            return 0

        if args.command == "label-balance":
            _print_json(
                diagnostics.label_balance(
                    conn,
                    config,
                    mode=args.mode,
                    subset_name=args.subset_name,
                    min_zone_positives=args.min_zone_positives,
                    min_corridor_positives=args.min_corridor_positives,
                )
            )
            return 0

        if args.command in {"compare-label-modes", "audit-label-modes"}:
            _print_json(
                diagnostics.compare_label_modes(
                    conn,
                    config,
                    base_mode=args.base_mode,
                    candidate_mode=args.candidate_mode,
                    subset_name=args.subset_name,
                    split=args.split,
                )
            )
            return 0

        if args.command in {"prepare-data", "sprint1"}:
            _print_json(
                _run_prepare_data(
                    conn,
                    config,
                    skip_bootstrap=args.skip_bootstrap,
                    label_mode=args.label_mode,
                )
            )
            return 0

        if args.command in {"benchmark", "run-baselines"}:
            results = benchmarks.run(
                conn,
                config,
                config_path=str(config_path),
                methods=_parse_csv_list(args.methods),
                budgets=_parse_csv_floats(args.budgets),
                run_tag=args.run_tag,
                split=args.split,
                subset_name=args.subset_name,
                evaluation_mode=args.evaluation_mode,
                truth_label_mode=args.truth_label_mode,
                overwrite=args.overwrite,
            )
            payload: dict[str, object] = {"runs": results}
            if results and not args.no_export_summary:
                payload["summary"] = reports.run(
                    conn,
                    config,
                    run_tag=args.run_tag or str(results[0]["run_tag"]),
                    methods=_parse_csv_list(args.methods),
                )
            _print_json(payload)
            return 0

        if args.command == "summarize-baselines":
            summary = reports.run(
                conn,
                config,
                run_tag=args.run_tag,
                methods=_parse_csv_list(args.methods),
            )
            _print_json(summary)
            return 0

        if args.command in {"inspect-html", "export-visual-inspection"}:
            summary = visual_inspection.run(
                conn,
                config,
                output_path=Path(args.output) if args.output else None,
                run_id=args.run_id,
                run_tag=args.run_tag,
                method=args.method,
                budget=args.budget,
                split=args.split,
                subset_name=args.subset_name,
                trajectory_ids=_parse_csv_ints(args.trajectory_ids),
                limit=args.limit,
                max_points_per_line=args.max_points_per_line,
                evaluation_mode=args.evaluation_mode,
                truth_label_mode=args.truth_label_mode,
            )
            _print_json(summary)
            return 0

        if args.command in {"inspect-qgis", "export-qgis-inspection"}:
            summary = qgis_export.run(
                conn,
                config,
                output_dir=Path(args.output_dir) if args.output_dir else None,
                run_id=args.run_id,
                run_tag=args.run_tag,
                method=args.method,
                budget=args.budget,
                split=args.split,
                subset_name=args.subset_name,
                trajectory_ids=_parse_csv_ints(args.trajectory_ids),
                limit=args.limit,
                include_points=not args.no_points,
                evaluation_mode=args.evaluation_mode,
                truth_label_mode=args.truth_label_mode,
            )
            _print_json(summary)
            return 0

    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
