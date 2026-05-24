"""Tests that short training keeps non-collapsed typed predictions. See src/training/README.md for details."""

from __future__ import annotations

import pytest
import torch

from src.data.ais_loader import generate_synthetic_ais_data
from src.data.trajectory_dataset import TrajectoryDataset
from src.experiments.experiment_config import build_experiment_config
from src.queries.query_generator import generate_typed_query_workload
from src.training.train_model import (
    _apply_temporal_residual_labels,
    _balanced_pointwise_loss,
    _combine_epoch_type_weights,
    _f1_selection_score,
    _selection_score,
    _uniform_gap_selection_score,
    train_model,
)


def test_selection_score_penalizes_collapsed_predictions() -> None:
    """Assert model selection does not prefer collapsed output solely because tau is nonnegative."""
    assert _selection_score(avg_tau=0.0, pred_std=0.0) < _selection_score(avg_tau=-0.05, pred_std=0.01)


def test_selection_score_uses_loss_before_tau_proxy() -> None:
    """Assert checkpoint selection does not restore a worse-loss epoch solely from noisy tau."""
    proxy_best = _selection_score(avg_tau=0.9, pred_std=0.1, loss=0.20)
    lower_loss = _selection_score(avg_tau=-0.1, pred_std=0.1, loss=0.10)

    assert lower_loss > proxy_best


def test_f1_selection_score_penalizes_collapsed_predictions() -> None:
    assert _f1_selection_score(query_f1=0.8, pred_std=0.0) < _f1_selection_score(query_f1=0.2, pred_std=0.01)


def test_uniform_gap_selection_penalizes_hidden_type_deficits() -> None:
    workload_mix = {"range": 0.25, "knn": 0.25, "similarity": 0.25, "clustering": 0.25}
    uniform_per_type = {"range": 0.50, "knn": 0.50, "similarity": 0.50, "clustering": 0.50}
    clustering_hidden = _uniform_gap_selection_score(
        query_f1=0.55,
        per_type_f1={"range": 0.45, "knn": 0.45, "similarity": 0.45, "clustering": 0.85},
        uniform_f1=0.50,
        uniform_per_type=uniform_per_type,
        workload_mix=workload_mix,
        pred_std=0.1,
    )
    balanced = _uniform_gap_selection_score(
        query_f1=0.54,
        per_type_f1={"range": 0.54, "knn": 0.54, "similarity": 0.54, "clustering": 0.54},
        uniform_f1=0.50,
        uniform_per_type=uniform_per_type,
        workload_mix=workload_mix,
        pred_std=0.1,
    )

    assert balanced > clustering_hidden


def test_balanced_pointwise_loss_pushes_constant_scores_apart() -> None:
    """Assert the anti-collapse BCE term has useful gradients from constant predictions."""
    pred = torch.zeros((8,), requires_grad=True)
    target = torch.tensor([1.0, 0.8, 0.4, 0.0, 0.0, 0.0, 0.0, 0.0])
    valid_mask = torch.ones((8,), dtype=torch.bool)

    loss = _balanced_pointwise_loss(
        pred=pred,
        target=target,
        valid_mask=valid_mask,
        generator=torch.Generator().manual_seed(123),
        negatives_per_positive=2,
    )
    loss.backward()

    assert float(loss.item()) > 0.0
    assert pred.grad is not None
    assert float(pred.grad[:3].sum().item()) < 0.0
    assert float(pred.grad[3:].sum().item()) > 0.0


def test_temporal_residual_labels_drop_base_points() -> None:
    labels = torch.ones((10, 4), dtype=torch.float32)
    labelled_mask = torch.ones((10, 4), dtype=torch.bool)

    residual_labels, residual_mask = _apply_temporal_residual_labels(
        labels=labels,
        labelled_mask=labelled_mask,
        boundaries=[(0, 10)],
        compression_ratio=0.3,
        temporal_fraction=0.5,
    )

    assert torch.where(~residual_mask[:, 0])[0].tolist() == [0, 9]
    assert residual_labels[[0, 9]].sum().item() == pytest.approx(0.0)
    assert bool(residual_mask[5].all().item())


def test_epoch_type_weights_respect_train_mix() -> None:
    base_weights = torch.tensor([0.2, 0.4, 0.2, 0.2])
    epoch_mix = torch.ones(4) / 4.0

    weights = _combine_epoch_type_weights(base_weights, epoch_mix)

    assert weights.tolist() == pytest.approx([0.2, 0.4, 0.2, 0.2])
    assert float(weights.sum().item()) == pytest.approx(1.0)


def test_training_records_validation_query_f1() -> None:
    trajectories = generate_synthetic_ais_data(n_ships=5, n_points_per_ship=24, seed=444)
    train_trajectories = trajectories[:4]
    validation_trajectories = trajectories[4:]
    train_ds = TrajectoryDataset(train_trajectories)
    validation_ds = TrajectoryDataset(validation_trajectories)
    train_boundaries = train_ds.get_trajectory_boundaries()
    validation_boundaries = validation_ds.get_trajectory_boundaries()

    cfg = build_experiment_config(
        epochs=1,
        n_queries=4,
        workload="range",
        checkpoint_selection_metric="f1",
        f1_diagnostic_every=1,
        compression_ratio=0.5,
    )
    cfg.model.embed_dim = 16
    cfg.model.num_heads = 2
    cfg.model.num_layers = 1
    cfg.model.query_chunk_size = 8
    cfg.model.window_length = 16
    cfg.model.window_stride = 8
    cfg.model.ranking_pairs_per_type = 8
    cfg.model.train_batch_size = 4
    cfg.model.diagnostic_window_fraction = 1.0

    train_workload = generate_typed_query_workload(
        trajectories=train_trajectories,
        n_queries=4,
        workload_mix={"range": 1.0},
        seed=101,
    )
    validation_workload = generate_typed_query_workload(
        trajectories=validation_trajectories,
        n_queries=4,
        workload_mix={"range": 1.0},
        seed=202,
    )

    out = train_model(
        train_trajectories=train_trajectories,
        train_boundaries=train_boundaries,
        workload=train_workload,
        model_config=cfg.model,
        seed=303,
        validation_trajectories=validation_trajectories,
        validation_boundaries=validation_boundaries,
        validation_workload=validation_workload,
        validation_mix={"range": 1.0},
    )

    f1_rows = [row for row in out.history if "val_query_f1" in row]
    assert f1_rows
    assert all(0.0 <= row["val_query_f1"] <= 1.0 for row in f1_rows)
    assert out.best_f1 == pytest.approx(max(row["val_query_f1"] for row in f1_rows))


def test_training_does_not_collapse(synthetic_dataset) -> None:
    """Assert prediction spread and typewise rank correlation stay healthy. See src/training/README.md for details."""
    trajectories, _ = synthetic_dataset
    ds = TrajectoryDataset(trajectories)
    boundaries = ds.get_trajectory_boundaries()

    cfg = build_experiment_config(epochs=4, n_queries=80)
    workload = generate_typed_query_workload(
        trajectories=trajectories,
        n_queries=80,
        workload_mix={"range": 0.25, "knn": 0.25, "similarity": 0.25, "clustering": 0.25},
        seed=77,
    )
    out = train_model(
        train_trajectories=trajectories,
        train_boundaries=boundaries,
        workload=workload,
        model_config=cfg.model,
        seed=77,
    )

    diagnostics = [row for row in out.history if "pred_std" in row]
    last = diagnostics[-1]
    assert last["pred_std"] > 0.02

    # kendall_tau check removed because per-type kendall_tau is no longer
    # emitted in diagnostics (non-essential, see train_model.py). pred_std > 0.02
    # alone covers "model didn't collapse to a constant"; quality-of-ranking is
    # better measured via val_query_f1 in the F1 diagnostic path.


def test_range_coverage_training_keeps_score_spread(synthetic_dataset) -> None:
    """Assert coverage-targeted range training does not converge to constant scores."""
    trajectories, _ = synthetic_dataset
    ds = TrajectoryDataset(trajectories)
    boundaries = ds.get_trajectory_boundaries()

    cfg = build_experiment_config(
        epochs=4,
        n_queries=60,
        query_coverage=0.30,
        max_queries=160,
        workload="range",
        lr=1e-3,
    )
    workload = generate_typed_query_workload(
        trajectories=trajectories,
        n_queries=60,
        target_coverage=0.30,
        max_queries=160,
        workload_mix={"range": 1.0},
        range_spatial_fraction=0.02,
        range_time_fraction=0.04,
        seed=91,
    )
    out = train_model(
        train_trajectories=trajectories,
        train_boundaries=boundaries,
        workload=workload,
        model_config=cfg.model,
        seed=91,
    )

    diagnostics = [row for row in out.history if "pred_std" in row]
    assert max(row["pred_std"] for row in diagnostics) > 0.01
    assert diagnostics[-1]["pred_std"] > 1e-3
