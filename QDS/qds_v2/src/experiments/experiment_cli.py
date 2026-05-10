"""CLI parsing helpers for the AIS-QDS v2 experiment runner. See src/experiments/README.md for details."""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    """Build experiment CLI parser. See src/experiments/README.md for details."""
    parser = argparse.ArgumentParser(description="Run AIS-QDS v2 experiment.")
    parser.add_argument("--csv_path", type=str, default=None)
    parser.add_argument("--train_csv_path", "--train_csv", dest="train_csv_path", type=str, default=None)
    parser.add_argument("--eval_csv_path", "--eval_csv", dest="eval_csv_path", type=str, default=None)
    parser.add_argument("--n_ships", type=int, default=24)
    parser.add_argument("--n_points", type=int, default=200)
    parser.add_argument("--n_queries", type=int, default=128)
    parser.add_argument(
        "--query_coverage",
        "--target_query_coverage",
        dest="query_coverage",
        type=float,
        default=None,
        help="Bias generated queries toward this point-coverage target while keeping --n_queries fixed. Accepts 0.30 or 30 for 30%%.",
    )
    parser.add_argument(
        "--max_queries",
        type=int,
        default=None,
        help="Deprecated compatibility option for coverage-based query generation.",
    )
    parser.add_argument(
        "--range_spatial_fraction",
        type=float,
        default=0.08,
        help="Range query half-width as a fraction of dataset lat/lon span. Lower values allow more queries without blanketing the dataset.",
    )
    parser.add_argument(
        "--range_time_fraction",
        type=float,
        default=0.15,
        help="Range query half-window as a fraction of dataset time span. Lower values allow more queries without blanketing the dataset.",
    )
    parser.add_argument(
        "--knn_k",
        type=int,
        default=12,
        help="Number of nearest trajectories returned by generated kNN queries.",
    )
    parser.add_argument(
        "--knn_t_half_window_fraction",
        type=float,
        default=0.25,
        help="kNN time half-window as a fraction of dataset time span. Default 0.25 (=6h on 1-day data) reproduces the legacy hardcoded behaviour. Lower this on multi-day data to keep absolute window size constant.",
    )
    parser.add_argument(
        "--similarity_time_fraction",
        type=float,
        default=0.04,
        help="Similarity query time half-window as a fraction of dataset time span. Default 0.04 (=~58min on 1-day) matches the legacy hardcoded constant. Scale down on multi-day data to keep absolute window size constant.",
    )
    parser.add_argument("--epochs", type=int, default=6)
    parser.add_argument("--lr", type=float, default=5e-4)
    parser.add_argument(
        "--pointwise_loss_weight",
        type=float,
        default=0.25,
        help="Weight for balanced pointwise BCE supervision alongside ranking loss.",
    )
    parser.add_argument(
        "--gradient_clip_norm",
        type=float,
        default=1.0,
        help="Max gradient norm. Set <=0 to disable clipping.",
    )
    parser.add_argument("--compression_ratio", type=float, default=0.2)
    parser.add_argument("--model_type", type=str, default="baseline", choices=["baseline", "turn_aware"])
    parser.add_argument("--workload", type=str, default="mixed")

    parser.add_argument("--train_workload_mix", type=str, default=None)
    parser.add_argument("--eval_workload_mix", type=str, default=None)
    parser.add_argument("--workload_mix_train", type=str, default=None)
    parser.add_argument("--workload_mix_eval", type=str, default=None)

    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--results_dir", type=str, default="results")
    parser.add_argument(
        "--early_stopping_patience",
        type=int,
        default=0,
        help="Stop training if avg Kendall tau does not improve for this many epochs. 0 disables.",
    )
    parser.add_argument(
        "--diagnostic_every",
        type=int,
        default=1,
        help="Run training diagnostics every N epochs. Use 1 so every epoch can be selected as best.",
    )
    parser.add_argument(
        "--diagnostic_window_fraction",
        type=float,
        default=0.2,
        help="Fraction of trajectory windows used for each diagnostic pass.",
    )
    parser.add_argument(
        "--checkpoint_selection_metric",
        type=str,
        default="loss",
        choices=["loss", "f1", "uniform_gap"],
        help="Select restored checkpoints by training loss, held-out query F1, or F1 with fair-uniform gap penalties.",
    )
    parser.add_argument(
        "--f1_diagnostic_every",
        type=int,
        default=0,
        help="Run held-out query-F1 diagnostics every N epochs. 0 disables unless checkpoint selection metric is f1 or uniform_gap.",
    )
    parser.add_argument(
        "--checkpoint_uniform_gap_weight",
        type=float,
        default=0.5,
        help="When checkpoint_selection_metric=uniform_gap, bonus/penalty weight for aggregate gap versus newUniformTemporal.",
    )
    parser.add_argument(
        "--checkpoint_type_penalty_weight",
        type=float,
        default=1.0,
        help="When checkpoint_selection_metric=uniform_gap, penalty weight for per-type F1 deficits versus newUniformTemporal.",
    )
    parser.add_argument(
        "--checkpoint_smoothing_window",
        type=int,
        default=1,
        help="Pick checkpoints by rolling-mean selection score over the last K diagnostic epochs. Reduces selection bias from noisy single-epoch F1. 1 = original single-epoch behavior; 5 = average over 5 latest diagnostic epochs.",
    )
    parser.add_argument(
        "--checkpoint_f1_variant",
        type=str,
        default="answer",
        choices=["answer", "combined"],
        help="Which F1 to use for validation/checkpoint selection. 'answer' = pure trajectory-set F1 (default). 'combined' = legacy answer_f1 * point_subset_f1 product (rewards keeping the eval-pipeline's support points; aligned with importance labels).",
    )
    parser.add_argument(
        "--mlqds_temporal_fraction",
        type=float,
        default=0.0,
        help="Fraction of the retained budget reserved for evenly spaced temporal base points before MLQDS score fill. Default 0.0 = pure learned scoring; raise to add a uniform spine.",
    )
    parser.add_argument(
        "--mlqds_diversity_bonus",
        type=float,
        default=0.05,
        help="Small spacing bonus for MLQDS fill candidates away from temporal base points.",
    )
    parser.add_argument(
        "--residual_label_mode",
        type=str,
        default="none",
        choices=["none", "temporal"],
        help="Use labels directly (default), or train only on points not already kept by the temporal base. 'none' is required for the 'score_coverage' simplifier.",
    )
    parser.add_argument(
        "--simplification_mode",
        type=str,
        default="score_coverage",
        choices=["score_coverage", "hybrid", "topk"],
        help="How learned scores are converted to a retained mask. 'score_coverage' (default): per-trajectory greedy with Gaussian density penalty (coverage-aware top-k). 'hybrid': temporal base + learned residual fill. 'topk': pure top-k.",
    )
    parser.add_argument(
        "--coverage_lambda",
        type=float,
        default=0.5,
        help="Weight of the density penalty in score_coverage selection. 0.0 reduces to top-k; larger values approach uniform spacing.",
    )
    parser.add_argument(
        "--coverage_sigma_fraction",
        type=float,
        default=0.5,
        help="Gaussian kernel width as a fraction of expected spacing (n/k). Larger values spread the penalty more broadly.",
    )
    parser.add_argument(
        "--use_cls_token",
        type=str,
        default="true",
        choices=["true", "false"],
        help="Whether the trajectory transformer uses a learnable CLS summary token consumed by cross-attention.",
    )
    parser.add_argument(
        "--knn_label_variant",
        type=str,
        default="legacy",
        choices=["legacy", "distance_weighted"],
        help="kNN label distribution. 'legacy' spreads gain equally across all in-window representatives of an answer trajectory; 'distance_weighted' concentrates label mass on the closest-to-anchor representative (matches what kNN F1 actually rewards: keeping at least one near-anchor point).",
    )
    parser.add_argument(
        "--range_label_variant",
        type=str,
        default="legacy",
        choices=["legacy", "uniform"],
        help="range label weighting. 'legacy' boosts boundary-crossing points 2x and adds cross-trajectory proximity prior; 'uniform' gives every in-box point equal label mass (matches range AnswerF1 = point-recall, doesn't reward boundary detection).",
    )
    parser.add_argument(
        "--save_model",
        type=str,
        default=None,
        help="Path to save trained model checkpoint (.pt). Disabled if not provided.",
    )
    parser.add_argument(
        "--save_queries_dir",
        type=str,
        default=None,
        help="Directory to save eval-workload queries as one GeoJSON per query type.",
    )
    parser.add_argument(
        "--save_simplified_dir",
        type=str,
        default=None,
        help="Directory to save MLQDS simplified trajectories as CSV.",
    )
    return parser
