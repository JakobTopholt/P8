"""Experiment configuration dataclasses for AIS-QDS v2. See src/experiments/README.md for details."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from typing import Any

import torch

LCG_MULTIPLIER = 6364136223846793005


def _known_dataclass_values(cls: type, data: dict[str, Any]) -> dict[str, Any]:
    """Drop stale serialized keys that are no longer part of a config dataclass."""
    allowed = {config_field.name for config_field in fields(cls)}
    return {key: value for key, value in data.items() if key in allowed}


@dataclass
class DataConfig:
    """Data loading and splitting configuration. See src/data/README.md for details."""

    n_ships: int = 24
    n_points_per_ship: int = 200
    csv_path: str | None = None
    train_csv_path: str | None = None
    eval_csv_path: str | None = None
    seed: int = 42
    train_fraction: float = 0.70
    val_fraction: float = 0.15

    def to_dict(self) -> dict[str, Any]:
        """Serialize config to a dictionary. See src/experiments/README.md for details."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataConfig":
        """Deserialize config from a dictionary. See src/experiments/README.md for details."""
        return cls(**data)


@dataclass
class QueryConfig:
    """Query generation and workload-mix configuration. See src/queries/README.md for details."""

    n_queries: int = 128
    target_coverage: float | None = None
    max_queries: int | None = None
    range_spatial_fraction: float = 0.08
    range_time_fraction: float = 0.15
    workload: str = "mixed"
    train_workload_mix: dict[str, float] = field(
        default_factory=lambda: {"range": 0.5, "knn": 0.5}
    )
    eval_workload_mix: dict[str, float] = field(
        default_factory=lambda: {"similarity": 0.5, "clustering": 0.5}
    )
    similarity_top_k: int = 5
    knn_k: int = 12
    knn_t_half_window_fraction: float = 0.25
    similarity_time_fraction: float = 0.04

    def to_dict(self) -> dict[str, Any]:
        """Serialize config to a dictionary. See src/experiments/README.md for details."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueryConfig":
        """Deserialize config from a dictionary. See src/experiments/README.md for details."""
        return cls(**data)


@dataclass
class ModelConfig:
    """Model architecture and training behavior config. See src/models/README.md for details."""

    embed_dim: int = 64
    num_heads: int = 4
    num_layers: int = 3
    type_embed_dim: int = 16
    query_chunk_size: int = 128
    dropout: float = 0.1
    window_length: int = 512
    window_stride: int = 256
    epochs: int = 6
    lr: float = 5e-4
    compression_ratio: float = 0.2
    model_type: str = "baseline"
    rank_margin: float = 0.05
    ranking_pairs_per_type: int = 96
    ranking_top_quantile: float = 0.80
    pointwise_loss_weight: float = 0.25
    gradient_clip_norm: float = 1.0
    l2_score_weight: float = 1e-4
    dirichlet_alpha: list[float] = field(default_factory=lambda: [1.0, 1.0, 1.0, 1.0])
    early_stopping_patience: int = 0
    train_batch_size: int = 16
    diagnostic_every: int = 1
    diagnostic_window_fraction: float = 0.2
    checkpoint_selection_metric: str = "loss"
    f1_diagnostic_every: int = 0
    checkpoint_uniform_gap_weight: float = 0.5
    checkpoint_type_penalty_weight: float = 1.0
    checkpoint_smoothing_window: int = 1
    checkpoint_f1_variant: str = "answer"
    mlqds_temporal_fraction: float = 0.0
    mlqds_diversity_bonus: float = 0.05
    residual_label_mode: str = "none"
    simplification_mode: str = "score_coverage"
    coverage_lambda: float = 0.5
    coverage_sigma_fraction: float = 0.5
    use_cls_token: bool = True
    knn_label_variant: str = "legacy"
    range_label_variant: str = "legacy"

    def to_dict(self) -> dict[str, Any]:
        """Serialize config to a dictionary. See src/experiments/README.md for details."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelConfig":
        """Deserialize config from a dictionary. See src/experiments/README.md for details."""
        return cls(**_known_dataclass_values(cls, data))


@dataclass
class BaselineConfig:
    """Baseline methods configuration. See src/evaluation/README.md for details."""

    include_oracle: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize config to a dictionary. See src/experiments/README.md for details."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BaselineConfig":
        """Deserialize config from a dictionary. See src/experiments/README.md for details."""
        return cls(**data)


@dataclass
class VisualizationConfig:
    """Visualization config. See src/visualization/README.md for details."""

    enabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize config to a dictionary. See src/experiments/README.md for details."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VisualizationConfig":
        """Deserialize config from a dictionary. See src/experiments/README.md for details."""
        return cls(**data)


@dataclass
class TypedQueryWorkload:
    """Typed query workload container. See src/queries/README.md for details."""

    query_features: torch.Tensor
    typed_queries: list[dict[str, Any]]
    type_ids: torch.Tensor
    coverage_fraction: float | None = None
    covered_points: int | None = None
    total_points: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize workload to a dictionary. See src/queries/README.md for details."""
        return {
            "query_features": self.query_features.tolist(),
            "typed_queries": self.typed_queries,
            "type_ids": self.type_ids.tolist(),
            "coverage_fraction": self.coverage_fraction,
            "covered_points": self.covered_points,
            "total_points": self.total_points,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TypedQueryWorkload":
        """Deserialize workload from a dictionary. See src/queries/README.md for details."""
        return cls(
            query_features=torch.tensor(data["query_features"], dtype=torch.float32),
            typed_queries=list(data["typed_queries"]),
            type_ids=torch.tensor(data["type_ids"], dtype=torch.long),
            coverage_fraction=data.get("coverage_fraction"),
            covered_points=data.get("covered_points"),
            total_points=data.get("total_points"),
        )


@dataclass
class ExperimentConfig:
    """Top-level experiment config container. See src/experiments/README.md for details."""

    data: DataConfig = field(default_factory=DataConfig)
    query: QueryConfig = field(default_factory=QueryConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    baselines: BaselineConfig = field(default_factory=BaselineConfig)
    visualization: VisualizationConfig = field(default_factory=VisualizationConfig)

    def to_dict(self) -> dict[str, Any]:
        """Serialize config to a dictionary. See src/experiments/README.md for details."""
        return {
            "data": self.data.to_dict(),
            "query": self.query.to_dict(),
            "model": self.model.to_dict(),
            "baselines": self.baselines.to_dict(),
            "visualization": self.visualization.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExperimentConfig":
        """Deserialize config from a dictionary. See src/experiments/README.md for details."""
        return cls(
            data=DataConfig.from_dict(data["data"]),
            query=QueryConfig.from_dict(data["query"]),
            model=ModelConfig.from_dict(data["model"]),
            baselines=BaselineConfig.from_dict(data["baselines"]),
            visualization=VisualizationConfig.from_dict(data["visualization"]),
        )


@dataclass
class SeedBundle:
    """Derived deterministic sub-seeds for experiment stages. See src/experiments/README.md for details."""

    split_seed: int
    train_query_seed: int
    eval_query_seed: int
    torch_seed: int


def build_experiment_config(
    n_ships: int = 24,
    n_points: int = 200,
    n_queries: int = 128,
    query_coverage: float | None = None,
    target_query_coverage: float | None = None,
    max_queries: int | None = None,
    range_spatial_fraction: float = 0.08,
    range_time_fraction: float = 0.15,
    epochs: int = 6,
    lr: float = 5e-4,
    pointwise_loss_weight: float = 0.25,
    gradient_clip_norm: float = 1.0,
    compression_ratio: float = 0.2,
    csv_path: str | None = None,
    train_csv_path: str | None = None,
    eval_csv_path: str | None = None,
    model_type: str = "baseline",
    workload: str = "mixed",
    train_workload_mix: dict[str, float] | None = None,
    eval_workload_mix: dict[str, float] | None = None,
    seed: int = 42,
    early_stopping_patience: int = 0,
    diagnostic_every: int = 1,
    diagnostic_window_fraction: float = 0.2,
    checkpoint_selection_metric: str = "loss",
    f1_diagnostic_every: int = 0,
    checkpoint_uniform_gap_weight: float = 0.5,
    checkpoint_type_penalty_weight: float = 1.0,
    checkpoint_smoothing_window: int = 1,
    checkpoint_f1_variant: str = "answer",
    knn_k: int = 12,
    knn_t_half_window_fraction: float = 0.25,
    similarity_time_fraction: float = 0.04,
    mlqds_temporal_fraction: float = 0.0,
    mlqds_diversity_bonus: float = 0.05,
    residual_label_mode: str = "none",
    simplification_mode: str = "score_coverage",
    coverage_lambda: float = 0.5,
    coverage_sigma_fraction: float = 0.5,
    use_cls_token: bool = True,
    knn_label_variant: str = "legacy",
    range_label_variant: str = "legacy",
    **_ignored_kwargs: Any,
) -> ExperimentConfig:
    """Build a structured experiment config from flat arguments. See src/experiments/README.md for details."""
    return ExperimentConfig(
        data=DataConfig(
            n_ships=n_ships,
            n_points_per_ship=n_points,
            csv_path=csv_path,
            train_csv_path=train_csv_path,
            eval_csv_path=eval_csv_path,
            seed=seed,
        ),
        query=QueryConfig(
            n_queries=n_queries,
            target_coverage=target_query_coverage if target_query_coverage is not None else query_coverage,
            max_queries=max_queries,
            range_spatial_fraction=range_spatial_fraction,
            range_time_fraction=range_time_fraction,
            workload=workload,
            train_workload_mix=train_workload_mix or {"range": 0.5, "knn": 0.5},
            eval_workload_mix=eval_workload_mix or {"similarity": 0.5, "clustering": 0.5},
            knn_k=knn_k,
            knn_t_half_window_fraction=knn_t_half_window_fraction,
            similarity_time_fraction=similarity_time_fraction,
        ),
        model=ModelConfig(
            epochs=epochs,
            lr=lr,
            pointwise_loss_weight=pointwise_loss_weight,
            gradient_clip_norm=gradient_clip_norm,
            compression_ratio=compression_ratio,
            model_type=model_type,
            early_stopping_patience=early_stopping_patience,
            diagnostic_every=diagnostic_every,
            diagnostic_window_fraction=diagnostic_window_fraction,
            checkpoint_selection_metric=checkpoint_selection_metric,
            f1_diagnostic_every=f1_diagnostic_every,
            checkpoint_uniform_gap_weight=checkpoint_uniform_gap_weight,
            checkpoint_type_penalty_weight=checkpoint_type_penalty_weight,
            checkpoint_smoothing_window=checkpoint_smoothing_window,
            checkpoint_f1_variant=checkpoint_f1_variant,
            mlqds_temporal_fraction=mlqds_temporal_fraction,
            mlqds_diversity_bonus=mlqds_diversity_bonus,
            residual_label_mode=residual_label_mode,
            simplification_mode=simplification_mode,
            coverage_lambda=coverage_lambda,
            coverage_sigma_fraction=coverage_sigma_fraction,
            use_cls_token=use_cls_token,
            knn_label_variant=knn_label_variant,
            range_label_variant=range_label_variant,
        ),
    )


def derive_seed_bundle(master_seed: int) -> SeedBundle:
    """Derive deterministic sub-seeds from a master seed. See src/experiments/README.md for details."""
    return SeedBundle(
        split_seed=(master_seed * LCG_MULTIPLIER + 1) & 0xFFFF_FFFF,
        train_query_seed=(master_seed * LCG_MULTIPLIER + 3) & 0xFFFF_FFFF,
        eval_query_seed=(master_seed * LCG_MULTIPLIER + 5) & 0xFFFF_FFFF,
        torch_seed=(master_seed * LCG_MULTIPLIER + 7) & 0xFFFF_FFFF,
    )
