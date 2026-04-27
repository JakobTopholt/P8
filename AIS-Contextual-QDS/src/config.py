"""Configuration models and loading helpers."""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .paths import resolve_project_path
from .postgres_tuning import normalize_session_profile
from .query_semantics import normalize_query_mode

_SCHEMA_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


@dataclass(frozen=True)
class ProjectConfig:
    """Project metadata."""

    name: str = "geofence-aware-query-driven-simplification"
    description: str = "AIS contextual query-driven simplification MVP"


@dataclass(frozen=True)
class DatabaseConfig:
    """Database input tables and destination schema."""

    url_env: str = "DATABASE_URL"
    schema: str = "ais_qds"
    source_points_table: str = "public.ais_points_cleaned"
    source_ts_column: str = "ts"
    source_ship_type_column: str = "ship_type"


@dataclass(frozen=True)
class ScopeConfig:
    """Study scope lock: region, vessel subset, and time window."""

    region_name: str = "great_belt_study_area"
    vessel_class_pattern: str = "%cargo%"
    window_start: str = "2026-01-01T00:00:00+00:00"
    window_end: str = "2026-01-29T00:00:00+00:00"


@dataclass(frozen=True)
class ContextConfig:
    """Context layers used by the MVP."""

    zone_names: list[str] = field(
        default_factory=lambda: [
            "zone_port_approach",
            "zone_anchor_or_waiting_area",
            "zone_narrow_passage_control",
        ]
    )
    corridor_name: str = "corridor_main_transit_lane"


@dataclass(frozen=True)
class TrajectoryConfig:
    """Trajectory building settings."""

    max_gap_minutes: int = 30
    max_implied_speed_knots: float = 60.0
    min_points: int = 20


@dataclass(frozen=True)
class QueryConfig:
    """Query-workload related settings."""

    retained_point_budgets: list[float] = field(default_factory=lambda: [0.10, 0.20, 0.30, 0.40, 0.50])
    min_corridor_overlap_meters: float = 1.0


@dataclass(frozen=True)
class SubsetConfig:
    """Development/evaluation subset settings."""

    subset_name: str = "great_belt_mvp"
    dev_size: int = 300
    eval_size: int = 300
    random_seed: int = 42


@dataclass(frozen=True)
class PathsConfig:
    """Project paths for SQL and outputs."""

    schema_sql_file: str = "sql/001_ais_qds_schema.sql"
    logs_dir: str = "results/logs"
    metrics_dir: str = "results/metrics"
    figures_dir: str = "results/figures"
    log_level: str = "INFO"


@dataclass(frozen=True)
class BaselineConfig:
    """Baseline runner settings for Sprint 2."""

    methods: list[str] = field(default_factory=lambda: ["uniform", "dp"])
    default_split: str = "dev"
    dp_search_iterations: int = 24
    insert_batch_size: int = 10_000


@dataclass(frozen=True)
class PerformanceConfig:
    """Query semantics and connection-level tuning defaults."""

    label_mode: str = "optimized"
    evaluation_mode: str = "optimized"
    session_profile: str = "laptop_safe"


@dataclass(frozen=True)
class AppConfig:
    """Top-level app config."""

    project: ProjectConfig = field(default_factory=ProjectConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    scope: ScopeConfig = field(default_factory=ScopeConfig)
    context: ContextConfig = field(default_factory=ContextConfig)
    trajectory: TrajectoryConfig = field(default_factory=TrajectoryConfig)
    queries: QueryConfig = field(default_factory=QueryConfig)
    subsets: SubsetConfig = field(default_factory=SubsetConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    baselines: BaselineConfig = field(default_factory=BaselineConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)


def _parse_datetime(value: str, field_name: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601. Got: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information.")
    return parsed


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    section_data = data.get(key, {})
    if not isinstance(section_data, dict):
        raise ValueError(f"Config section '{key}' must be a mapping.")
    return section_data


def validate_config(config: AppConfig) -> None:
    """Validate cross-field constraints."""
    if not _SCHEMA_PATTERN.match(config.database.schema):
        raise ValueError("database.schema must be a valid SQL schema identifier.")

    if not _TABLE_PATTERN.match(config.database.source_points_table):
        raise ValueError("database.source_points_table must be schema.table or table.")

    if not _SCHEMA_PATTERN.match(config.database.source_ts_column):
        raise ValueError("database.source_ts_column must be a valid identifier.")

    if not _SCHEMA_PATTERN.match(config.database.source_ship_type_column):
        raise ValueError("database.source_ship_type_column must be a valid identifier.")

    if not config.scope.region_name:
        raise ValueError("scope.region_name cannot be empty.")

    if not config.scope.vessel_class_pattern:
        raise ValueError("scope.vessel_class_pattern cannot be empty.")

    window_start = _parse_datetime(config.scope.window_start, "scope.window_start")
    window_end = _parse_datetime(config.scope.window_end, "scope.window_end")
    if window_start >= window_end:
        raise ValueError("scope.window_start must be earlier than scope.window_end.")

    if len(config.context.zone_names) != 3:
        raise ValueError("context.zone_names must contain exactly 3 zones for MVP scope lock.")

    if len(set(config.context.zone_names)) != len(config.context.zone_names):
        raise ValueError("context.zone_names must be unique.")

    if any(not zone for zone in config.context.zone_names):
        raise ValueError("context.zone_names cannot contain empty names.")

    if not config.context.corridor_name:
        raise ValueError("context.corridor_name cannot be empty.")

    if config.trajectory.max_gap_minutes <= 0:
        raise ValueError("trajectory.max_gap_minutes must be > 0.")

    if config.trajectory.max_implied_speed_knots <= 0:
        raise ValueError("trajectory.max_implied_speed_knots must be > 0.")

    if config.trajectory.min_points < 2:
        raise ValueError("trajectory.min_points must be at least 2.")

    budgets = config.queries.retained_point_budgets
    if not budgets:
        raise ValueError("queries.retained_point_budgets cannot be empty.")

    if budgets != sorted(budgets):
        raise ValueError("queries.retained_point_budgets must be sorted in ascending order.")

    for budget in budgets:
        if not 0 < budget <= 1:
            raise ValueError("Each retained-point budget must be in (0, 1].")

    if config.queries.min_corridor_overlap_meters < 0:
        raise ValueError("queries.min_corridor_overlap_meters must be >= 0.")

    if not 200 <= config.subsets.dev_size <= 500:
        raise ValueError("subsets.dev_size must be between 200 and 500 for MVP.")

    if config.subsets.eval_size <= 0:
        raise ValueError("subsets.eval_size must be > 0.")

    if not config.baselines.methods:
        raise ValueError("baselines.methods cannot be empty.")

    valid_splits = {"all", "dev", "eval"}
    if config.baselines.default_split not in valid_splits:
        raise ValueError("baselines.default_split must be one of all/dev/eval.")

    if config.baselines.dp_search_iterations <= 0:
        raise ValueError("baselines.dp_search_iterations must be > 0.")

    if config.baselines.insert_batch_size <= 0:
        raise ValueError("baselines.insert_batch_size must be > 0.")

    normalize_query_mode(config.performance.label_mode, default="optimized")
    normalize_query_mode(config.performance.evaluation_mode, default="optimized")
    normalize_session_profile(config.performance.session_profile)

    schema_sql_path = resolve_project_path(config.paths.schema_sql_file)
    if not schema_sql_path.exists():
        raise ValueError(f"paths.schema_sql_file does not exist: {schema_sql_path}")


def load_config(config_path: Path) -> AppConfig:
    """Load and validate YAML config."""
    with config_path.open("r", encoding="utf-8") as file_obj:
        raw = yaml.safe_load(file_obj) or {}

    if not isinstance(raw, dict):
        raise ValueError("Top-level config must be a mapping.")

    config = AppConfig(
        project=ProjectConfig(**_section(raw, "project")),
        database=DatabaseConfig(**_section(raw, "database")),
        scope=ScopeConfig(**_section(raw, "scope")),
        context=ContextConfig(**_section(raw, "context")),
        trajectory=TrajectoryConfig(**_section(raw, "trajectory")),
        queries=QueryConfig(**_section(raw, "queries")),
        subsets=SubsetConfig(**_section(raw, "subsets")),
        paths=PathsConfig(**_section(raw, "paths")),
        baselines=BaselineConfig(**_section(raw, "baselines")),
        performance=PerformanceConfig(**_section(raw, "performance")),
    )
    validate_config(config)
    return config


def _read_env_value_from_file(env_path: Path, key: str) -> str | None:
    if not env_path.exists():
        return None

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        lhs, rhs = line.split("=", 1)
        if lhs.strip() != key:
            continue

        value = rhs.strip().strip("'").strip('"')
        if value:
            return value
    return None


def read_database_url(config: AppConfig) -> str:
    """Resolve database URL from environment."""
    database_url = os.getenv(config.database.url_env)
    if not database_url:
        env_paths = [
            resolve_project_path(".env"),
            resolve_project_path("../.env"),
        ]
        for env_path in env_paths:
            database_url = _read_env_value_from_file(env_path, config.database.url_env)
            if database_url:
                return database_url

        raise RuntimeError(
            f"Environment variable {config.database.url_env!r} is not set. "
            "Export it before running the pipeline, or define it in .env."
        )
    return database_url
