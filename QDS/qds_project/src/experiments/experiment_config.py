"""Shared configuration and result types for the AIS experiment."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import torch


@dataclass(frozen=True)
class DataConfig:
    """Configuration for loading or generating AIS trajectories."""

    n_ships: int = 10
    n_points_per_ship: int = 100
    csv_path: str | None = None
    save_csv: bool = False


@dataclass(frozen=True)
class QueryConfig:
    """Configuration for generating query workloads."""

    workload: str = "density"
    n_queries: int = 100
    density_ratio: float = 0.7
    spatial_fraction: float = 0.03
    temporal_fraction: float = 0.10
    spatial_lower_quantile: float = 0.01
    spatial_upper_quantile: float = 0.99


@dataclass
class TypedQueryWorkload:
    """Holds both a range-query tensor (for training) and typed query list (for evaluation).

    The ``range_queries`` tensor uses the standard [M, 6] format and is used
    for importance-label computation and model training (which require the
    tensor-based API).  The ``typed_queries`` list is used for the final
    evaluation step and can contain any mix of query types.
    """

    range_queries: torch.Tensor
    """[M, 6] range-query tensor derived from the same spatial parameters."""
    typed_queries: list[dict[str, Any]]
    """Typed query list for evaluation (may contain multiple query types)."""


@dataclass(frozen=True)
class ModelConfig:
    """Configuration for training and simplification."""

    epochs: int = 50
    threshold: float = 0.5
    target_ratio: float | None = None
    compression_ratio: float | None = 0.2
    min_points_per_trajectory: int = 5
    max_train_points: int | None = None
    model_max_points: int | None = 300_000
    point_batch_size: int = 50_000
    importance_chunk_size: int = 200_000
    model_type: str = "baseline"
    turn_bias_weight: float = 0.1
    turn_score_method: str = "heading"
    max_query_error: float | None = None
    max_search_iterations: int = 20
    error_tolerance: float = 1e-3


@dataclass(frozen=True)
class BaselineConfig:
    """Configuration for baseline execution."""

    dp_max_points: int = 200_000
    skip_baselines: bool = False


@dataclass(frozen=True)
class VisualizationConfig:
    """Configuration for visualization generation."""

    skip_visualizations: bool = False
    max_visualization_points: int = 200_000
    max_visualization_ships: int = 200
    max_points_per_ship_plot: int = 2_000


@dataclass(frozen=True)
class ExperimentConfig:
    """Top-level experiment configuration object."""

    data: DataConfig = field(default_factory=DataConfig)
    query: QueryConfig = field(default_factory=QueryConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    baselines: BaselineConfig = field(default_factory=BaselineConfig)
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)


@dataclass(frozen=True)
class MethodMetrics:
    """Evaluation metrics for a simplified method."""

    query_error: float
    compression_ratio: float
    latency_ms: float


@dataclass(frozen=True)
class ModelSimplificationResult:
    """Simplification outputs for one trained model variant."""

    simplified_points: torch.Tensor
    retained_mask: torch.Tensor
    scores: torch.Tensor
    label: str


def build_experiment_config(
    n_ships: int,
    n_points: int,
    n_queries: int,
    epochs: int,
    threshold: float,
    target_ratio: float | None,
    compression_ratio: float | None,
    min_points_per_trajectory: int,
    max_train_points: int | None,
    model_max_points: int | None,
    point_batch_size: int,
    importance_chunk_size: int,
    dp_max_points: int,
    skip_baselines: bool,
    skip_visualizations: bool,
    max_visualization_points: int,
    max_visualization_ships: int,
    max_points_per_ship_plot: int,
    csv_path: str | None,
    save_csv: bool,
    workload: str,
    density_ratio: float,
    query_spatial_fraction: float,
    query_temporal_fraction: float,
    query_spatial_lower_quantile: float,
    query_spatial_upper_quantile: float,
    model_type: str,
    turn_bias_weight: float,
    turn_score_method: str,
    max_query_error: float | None = None,
    max_search_iterations: int = 20,
    error_tolerance: float = 1e-3,
) -> ExperimentConfig:
    """Build a single structured config from the flat argument list."""
    return ExperimentConfig(
        data=DataConfig(
            n_ships=n_ships,
            n_points_per_ship=n_points,
            csv_path=csv_path,
            save_csv=save_csv,
        ),
        query=QueryConfig(
            workload=workload,
            n_queries=n_queries,
            density_ratio=density_ratio,
            spatial_fraction=query_spatial_fraction,
            temporal_fraction=query_temporal_fraction,
            spatial_lower_quantile=query_spatial_lower_quantile,
            spatial_upper_quantile=query_spatial_upper_quantile,
        ),
        model=ModelConfig(
            epochs=epochs,
            threshold=threshold,
            target_ratio=target_ratio,
            compression_ratio=compression_ratio,
            min_points_per_trajectory=min_points_per_trajectory,
            max_train_points=max_train_points,
            model_max_points=model_max_points,
            point_batch_size=point_batch_size,
            importance_chunk_size=importance_chunk_size,
            model_type=model_type,
            turn_bias_weight=turn_bias_weight,
            turn_score_method=turn_score_method,
            max_query_error=max_query_error,
            max_search_iterations=max_search_iterations,
            error_tolerance=error_tolerance,
        ),
        baselines=BaselineConfig(
            dp_max_points=dp_max_points,
            skip_baselines=skip_baselines,
        ),
        visualization=VisualizationConfig(
            skip_visualizations=skip_visualizations,
            max_visualization_points=max_visualization_points,
            max_visualization_ships=max_visualization_ships,
            max_points_per_ship_plot=max_points_per_ship_plot,
        ),
    )
