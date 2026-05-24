"""Tests pipeline reporting of higher-is-better aggregate F1. See src/evaluation/README.md for details."""

from __future__ import annotations

from src.experiments.experiment_config import build_experiment_config
from src.experiments.experiment_pipeline_helpers import run_experiment_pipeline


def test_pipeline_reports_f1_scores(synthetic_dataset, tmp_path) -> None:
    """Assert matched-workload metrics use F1 fields and valid score polarity."""
    trajectories, _ = synthetic_dataset
    cfg = build_experiment_config(n_queries=64, epochs=3)
    mix = {"range": 0.7, "knn": 0.3}

    out = run_experiment_pipeline(
        config=cfg,
        trajectories=trajectories,
        train_mix=mix,
        eval_mix=mix,
        results_dir=str(tmp_path),
        save_simplified_dir=str(tmp_path / "simplified"),
    )

    ml = out.metrics_dump["matched"]["MLQDS"]["aggregate_f1"]
    uni = out.metrics_dump["matched"]["uniform"]["aggregate_f1"]
    assert 0.0 <= ml <= 1.0
    assert 0.0 <= uni <= 1.0
    assert "AnswerF1" in out.matched_table
    assert "CombinedF1" in out.matched_table
    assert "AggregateErr" not in out.matched_table
    assert "aggregate_error" not in out.metrics_dump["matched"]["MLQDS"]
    assert "per_type_f1" in out.metrics_dump["matched"]["MLQDS"]

    # F1 is higher-is-better, so callers should rank with max(), not min().
    scores = {name: metrics["aggregate_f1"] for name, metrics in out.metrics_dump["matched"].items()}
    assert scores[max(scores, key=scores.get)] >= scores[min(scores, key=scores.get)]
    assert (tmp_path / "simplified" / "ML_simplified_eval.csv").exists()
    assert not (tmp_path / "simplified" / "ML_simplified_train.csv").exists()
    assert not (tmp_path / "simplified" / "ML_simplified.csv").exists()
