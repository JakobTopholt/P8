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
from .pipelines import baselines, bootstrap, context_loader, labels, qgis_export, reports, status, subsets, trajectories, visual_inspection

LOGGER = logging.getLogger(__name__)


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


def build_parser() -> argparse.ArgumentParser:
    """Build top-level argument parser."""
    parser = argparse.ArgumentParser(
        description="AIS contextual query-driven simplification pipeline bootstrap."
    )
    parser.add_argument(
        "--config",
        default="configs/mvp.example.yaml",
        help="Path to YAML config file (relative to AIS-Contextual-QDS root by default).",
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

    subset_parser = subparsers.add_parser(
        "create-dev-subset",
        help="Create deterministic dev/eval subset assignment.",
    )
    subset_parser.add_argument(
        "--append",
        action="store_true",
        help="Do not delete existing subset rows with the same subset_name.",
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

    sprint1_parser = subparsers.add_parser(
        "sprint1",
        help="Run bootstrap + trajectory build + labels + subset + status.",
    )
    sprint1_parser.add_argument(
        "--skip-bootstrap",
        action="store_true",
        help="Skip schema bootstrap step.",
    )

    baseline_parser = subparsers.add_parser(
        "run-baselines",
        help="Run Sprint 2 baselines (uniform, dp) across budget ratios.",
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
        "export-visual-inspection",
        help="Export a self-contained HTML report for raw/context/simplified trajectory inspection.",
    )
    inspect_parser.add_argument(
        "--output",
        default=None,
        help="Output HTML path. Defaults to results/figures/inspection_*.html.",
    )
    inspect_parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Optional simplification run_id to compare against raw trajectories.",
    )
    inspect_parser.add_argument(
        "--run-tag",
        default=None,
        help="Optional run_tag filter for selecting a simplification run.",
    )
    inspect_parser.add_argument(
        "--method",
        default=None,
        help="Optional method filter for selecting a simplification run, e.g. uniform or dp.",
    )
    inspect_parser.add_argument(
        "--budget",
        type=float,
        default=None,
        help="Optional budget filter for selecting a simplification run, e.g. 0.1.",
    )
    inspect_parser.add_argument(
        "--split",
        choices=["all", "dev", "eval"],
        default="dev",
        help="Subset split to sample when --trajectory-ids is not provided.",
    )
    inspect_parser.add_argument(
        "--subset-name",
        default=None,
        help="Subset name override.",
    )
    inspect_parser.add_argument(
        "--trajectory-ids",
        default=None,
        help="Comma-separated trajectory IDs to inspect.",
    )
    inspect_parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="Maximum number of trajectories to export.",
    )
    inspect_parser.add_argument(
        "--max-points-per-line",
        type=int,
        default=1500,
        help="Maximum points rendered per raw/simplified SVG line for readability.",
    )

    qgis_parser = subparsers.add_parser(
        "export-qgis-inspection",
        help="Export GeoJSON layers and a QGIS project for trajectory/context inspection.",
    )
    qgis_parser.add_argument(
        "--output-dir",
        default=None,
        help="Output folder. Defaults to results/figures/qgis_inspection_*.",
    )
    qgis_parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Optional simplification run_id to compare against raw trajectories.",
    )
    qgis_parser.add_argument(
        "--run-tag",
        default=None,
        help="Optional run_tag filter for selecting a simplification run.",
    )
    qgis_parser.add_argument(
        "--method",
        default=None,
        help="Optional method filter for selecting a simplification run, e.g. uniform or dp.",
    )
    qgis_parser.add_argument(
        "--budget",
        type=float,
        default=None,
        help="Optional budget filter for selecting a simplification run, e.g. 0.1.",
    )
    qgis_parser.add_argument(
        "--split",
        choices=["all", "dev", "eval"],
        default="dev",
        help="Subset split to sample when --trajectory-ids is not provided.",
    )
    qgis_parser.add_argument(
        "--subset-name",
        default=None,
        help="Subset name override.",
    )
    qgis_parser.add_argument(
        "--trajectory-ids",
        default=None,
        help="Comma-separated trajectory IDs to inspect.",
    )
    qgis_parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="Maximum number of trajectories to export.",
    )
    qgis_parser.add_argument(
        "--no-points",
        action="store_true",
        help="Skip raw/simplified point layers and export trajectory lines only.",
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
        if args.command == "bootstrap":
            bootstrap.run(conn, config)
            return 0

        if args.command == "build-trajectories":
            summary = trajectories.run(conn, config, truncate=not args.append)
            _print_json(summary)
            return 0

        if args.command == "compute-labels":
            summary = labels.run(conn, config, truncate=not args.append)
            _print_json(summary)
            return 0

        if args.command == "create-dev-subset":
            summary = subsets.run(conn, config, truncate=not args.append)
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

        if args.command == "sprint1":
            if not args.skip_bootstrap:
                bootstrap.run(conn, config)
            trajectories.run(conn, config, truncate=True)
            labels.run(conn, config, truncate=True)
            subsets.run(conn, config, truncate=True)
            _print_json(status.run(conn, config))
            return 0

        if args.command == "run-baselines":
            results = baselines.run(
                conn,
                config,
                config_path=str(config_path),
                methods=_parse_csv_list(args.methods),
                budgets=_parse_csv_floats(args.budgets),
                run_tag=args.run_tag,
                split=args.split,
                subset_name=args.subset_name,
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

        if args.command == "export-visual-inspection":
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
            )
            _print_json(summary)
            return 0

        if args.command == "export-qgis-inspection":
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
            )
            _print_json(summary)
            return 0

    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
