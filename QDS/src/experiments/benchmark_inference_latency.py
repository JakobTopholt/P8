"""Inference-latency benchmark for AIS-QDS v2.

Times JUST the .simplify() call for MLQDS and DouglasPeucker on a single
eval day, with varying query counts. No F1, no metric computation, no eval
table — pure simplification latency.

Each query count is run twice: first call warms up CUDA kernels and any
lazy initialization; second call is the reported number. CUDA is
synchronized before and after each timed call so GPU work isn't pipelined
into the next call.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import torch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from src.data.ais_loader import load_ais_csv
from src.data.trajectory_dataset import TrajectoryDataset
from src.evaluation.baselines import DouglasPeuckerMethod, MLQDSMethod
from src.queries.query_generator import generate_typed_query_workload
from src.queries.query_types import NUM_QUERY_TYPES
from src.training.train_model import TrainingOutputs
from src.training.training_pipeline import load_checkpoint


def _sync():
    if torch.cuda.is_available():
        torch.cuda.synchronize()


def _time(label: str, fn) -> float:
    _sync()
    t0 = time.perf_counter()
    out = fn()
    _sync()
    dt = time.perf_counter() - t0
    print(f"  {label}: {dt:.3f}s")
    return dt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--csv_path", required=True)
    parser.add_argument("--query_counts", type=str, default="1,10,150")
    parser.add_argument("--compression_ratio", type=float, default=0.05)
    parser.add_argument("--workload_mix", type=str, default="range=1.0")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--range_spatial_fraction", type=float, default=0.015)
    parser.add_argument("--range_time_fraction", type=float, default=0.0833)
    parser.add_argument("--similarity_time_fraction", type=float, default=0.0833)
    parser.add_argument("--knn_t_half_window_fraction", type=float, default=0.0833)
    parser.add_argument("--knn_k", type=int, default=50)
    args = parser.parse_args()

    query_counts = [int(x) for x in args.query_counts.split(",")]
    mix = {k.strip(): float(v) for k, v in [pair.split("=") for pair in args.workload_mix.split(",")]}

    print(f"[load-checkpoint] {args.checkpoint}")
    artifacts = load_checkpoint(args.checkpoint)
    saved_cfg = artifacts.config

    print(f"[load-data] {args.csv_path}")
    t0 = time.perf_counter()
    trajectories, _ = load_ais_csv(args.csv_path, return_mmsis=True)
    print(f"  loaded {len(trajectories)} trajectories in {time.perf_counter() - t0:.2f}s")

    dataset = TrajectoryDataset(trajectories)
    points = dataset.get_all_points()
    boundaries = dataset.get_trajectory_boundaries()
    print(f"[dataset] points={points.shape[0]} trajectories={len(boundaries)}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[device] {device}")
    if torch.cuda.is_available():
        print(f"[device] {torch.cuda.get_device_name(0)}")

    # Keep points on CPU — windowed_predict + build_trajectory_windows handle
    # device transfers internally. Moving points to GPU here trips a
    # cpu/cuda device-mismatch in build_trajectory_windows' zero-pad cat.
    points_for_mlqds = points

    trained = TrainingOutputs(
        model=artifacts.model.to(device),
        scaler=artifacts.scaler,
        labels=torch.zeros((1, NUM_QUERY_TYPES), dtype=torch.float32),
        labelled_mask=torch.zeros((1,), dtype=torch.bool),
        history=[],
        epochs_trained=int(artifacts.epochs_trained),
    )

    # ---------- DouglasPeucker (independent of query count) ----------
    print("\n[DP] simplify (CPU, no queries needed)")
    dp = DouglasPeuckerMethod()
    _time("DP warmup", lambda: dp.simplify(points, boundaries, args.compression_ratio))
    dp_time = _time("DP", lambda: dp.simplify(points, boundaries, args.compression_ratio))

    # ---------- MLQDS for each query count ----------
    mlqds_results: dict[int, float] = {}
    for q in query_counts:
        print(f"\n[MLQDS] simplify with {q} queries")
        workload = generate_typed_query_workload(
            trajectories=trajectories,
            n_queries=q,
            workload_mix=mix,
            seed=args.seed,
            target_coverage=None,
            max_queries=None,
            range_spatial_fraction=args.range_spatial_fraction,
            range_time_fraction=args.range_time_fraction,
            knn_k=args.knn_k,
            knn_t_half_window_fraction=args.knn_t_half_window_fraction,
            similarity_time_fraction=args.similarity_time_fraction,
            single_day_clip=True,
        )
        print(f"  generated {len(workload.typed_queries)} queries")
        mlqds = MLQDSMethod(
            name="MLQDS",
            trained=trained,
            workload=workload,
            workload_mix=mix,
            temporal_fraction=float(getattr(saved_cfg.model, "mlqds_temporal_fraction", 0.30)),
            diversity_bonus=float(getattr(saved_cfg.model, "mlqds_diversity_bonus", 0.05)),
            simplification_mode=str(getattr(saved_cfg.model, "simplification_mode", "score_coverage")),
            coverage_lambda=float(getattr(saved_cfg.model, "coverage_lambda", 0.5)),
            coverage_sigma_fraction=float(getattr(saved_cfg.model, "coverage_sigma_fraction", 0.5)),
            length_preservation_weight=float(getattr(saved_cfg.model, "length_preservation_weight", 0.0)),
        )
        _time("warmup", lambda: mlqds.simplify(points_for_mlqds, boundaries, args.compression_ratio))
        t = _time(f"MLQDS Q={q}", lambda: mlqds.simplify(points_for_mlqds, boundaries, args.compression_ratio))
        mlqds_results[q] = t

    # ---------- Summary ----------
    print("\n========== SUMMARY ==========")
    print(f"DP                : {dp_time:.3f}s")
    for q in query_counts:
        ratio = mlqds_results[q] / dp_time if dp_time > 0 else float("inf")
        print(f"MLQDS Q={q:<4}     : {mlqds_results[q]:.3f}s   ({ratio:.1f}x vs DP)")
    print("==============================")


if __name__ == "__main__":
    main()
