"""Run a saved AIS-QDS v2 model on a new day's preprocessed CSV (no training).

Loads a .pt checkpoint produced by run_ais_experiment.py (save_checkpoint) and
evaluates MLQDS against baselines on the supplied CSV. No gradient updates are
performed, so this is safe for a local CPU/GPU machine.

Example (PowerShell, from repo root):

    python -m src.experiments.run_inference `
        --checkpoint src/models/saved_models/model_2026-02-05_q25_range_766395.pt `
        --csv_path ../../AISDATA/preprocessed_AIS_files/preprocessed_2026-02-08.csv `
        --n_queries 100 `
        --results_dir results/inference_2026-02-08_range

Run this from ``QDS/qds_v2`` so the ``src`` package resolves.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import torch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from src.data.ais_loader import load_ais_csv
from src.data.trajectory_dataset import TrajectoryDataset
from src.evaluation.baselines import (
    DouglasPeuckerMethod,
    MLQDSMethod,
    NewUniformTemporalMethod,
)
from src.evaluation.evaluate_methods import (
    evaluate_method,
    print_geometric_distortion_table,
    print_length_retention_whole_set_table,
    print_method_comparison_table,
)
from src.experiments.geojson_writers import read_queries_geojson, write_queries_geojson, write_simplified_csv
from src.queries.query_generator import query_coverage_mask
from src.queries.query_types import pad_query_features
from src.experiments.experiment_config import TypedQueryWorkload
from src.queries.query_generator import generate_typed_query_workload
from src.queries.query_types import NUM_QUERY_TYPES
from src.training.train_model import TrainingOutputs
from src.training.training_pipeline import load_checkpoint


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Evaluate a saved AIS-QDS v2 model on a new CSV.")
    p.add_argument("--checkpoint", required=True, help="Path to saved .pt checkpoint.")
    p.add_argument("--csv_path", required=True, help="Preprocessed AIS CSV to evaluate on.")
    p.add_argument("--n_queries", type=int, default=100, help="Queries to generate for evaluation.")
    p.add_argument(
        "--query_coverage",
        "--target_query_coverage",
        dest="query_coverage",
        type=float,
        default=None,
        help="Bias generated queries toward this point-coverage target while keeping --n_queries fixed. Accepts 0.30 or 30 for 30%%.",
    )
    p.add_argument(
        "--max_queries",
        type=int,
        default=None,
        help="Deprecated compatibility option for coverage-based query generation.",
    )
    p.add_argument(
        "--range_spatial_fraction",
        type=float,
        default=0.08,
        help="Range query half-width as a fraction of dataset lat/lon span.",
    )
    p.add_argument(
        "--range_time_fraction",
        type=float,
        default=0.15,
        help="Range query half-window as a fraction of dataset time span.",
    )
    p.add_argument("--knn_k", type=int, default=12, help="Number of nearest trajectories in generated kNN queries.")
    p.add_argument(
        "--similarity_time_fraction",
        type=float,
        default=0.04,
        help="Similarity query time half-window as a fraction of dataset time span. Match the training value (0.0833 for 7-day experiments).",
    )
    p.add_argument(
        "--similarity_top_k",
        type=int,
        default=5,
        help="Number of trajectories returned by each generated similarity query. Match the training value (e.g. 50 for top_k=50 models).",
    )
    p.add_argument(
        "--workload_mix",
        type=str,
        default=None,
        help="Override eval workload mix, e.g. 'range=1.0' or 'range=0.5,knn=0.5'. "
             "Default: use mix saved in checkpoint; else fall back to 100%% range.",
    )
    p.add_argument(
        "--compression_ratio",
        type=float,
        default=None,
        help="Override compression ratio. Default: use value saved in checkpoint.",
    )
    p.add_argument("--seed", type=int, default=42, help="Seed for query generation.")
    p.add_argument("--results_dir", type=str, default="results/inference", help="Where to write tables / metrics.")
    p.add_argument(
        "--save_queries_dir",
        type=str,
        default=None,
        help="If set, write eval-workload queries as GeoJSON to this directory.",
    )
    p.add_argument(
        "--save_simplified_dir",
        type=str,
        default=None,
        help="If set, write MLQDS-simplified trajectory CSV here.",
    )
    p.add_argument(
        "--max_trajectories",
        type=int,
        default=None,
        help="Optional cap on trajectories loaded (useful for quick smoke tests on a laptop).",
    )
    p.add_argument(
        "--queries_from",
        type=str,
        default=None,
        help="Directory containing queries_{type}.geojson files written by --save_queries_dir. "
             "When set, eval uses these EXACT saved queries instead of regenerating, "
             "so a re-run can be compared to the training-time validation numbers. "
             "Skips --n_queries / --query_coverage / --seed / range_*/knn_* / similarity_* flags.",
    )
    p.add_argument(
        "--no_query_model",
        action="store_true",
        help="Run MLQDS with an EMPTY workload (no queries fed to the model) while still using the generated --n_queries workload for F1/length-retention evaluation. Tests whether a single-workload model has learned a generalizable importance signal without per-day query generation.",
    )
    return p


def _parse_mix(text: str) -> dict[str, float]:
    mix: dict[str, float] = {}
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid workload mix token '{item}'. Expected 'type=weight'.")
        k, v = item.split("=", 1)
        mix[k.strip().lower()] = float(v.strip())
    if not mix:
        raise ValueError("Empty workload mix.")
    return mix


def _resolve_eval_mix(cli_mix: str | None, checkpoint_mix: dict[str, float] | None) -> dict[str, float]:
    if cli_mix:
        return _parse_mix(cli_mix)
    if checkpoint_mix:
        return dict(checkpoint_mix)
    return {"range": 1.0}


def main() -> None:
    args = _build_parser().parse_args()

    print(f"[load-checkpoint] {args.checkpoint}", flush=True)
    artifacts = load_checkpoint(args.checkpoint)
    saved_cfg = artifacts.config

    # load_checkpoint puts the model on CPU. Move it to GPU when available —
    # full-eval inference on millions of points is CPU-bound otherwise.
    # windowed_predict moves window tensors / queries to the model's device.
    inference_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    artifacts.model.to(inference_device)
    print(f"[device] inference on {inference_device}", flush=True)
    if torch.cuda.is_available():
        print(f"[device] {torch.cuda.get_device_name(0)}", flush=True)
    saved_train_mix = artifacts.train_workload_mix
    saved_eval_mix = artifacts.eval_workload_mix
    print(
        f"  model_type={saved_cfg.model.model_type}  "
        f"epochs_trained={artifacts.epochs_trained}  "
        f"train_mix={saved_train_mix}  eval_mix={saved_eval_mix}",
        flush=True,
    )

    eval_mix = _resolve_eval_mix(args.workload_mix, saved_eval_mix or saved_train_mix)
    compression_ratio = (
        float(args.compression_ratio) if args.compression_ratio is not None else float(saved_cfg.model.compression_ratio)
    )
    print(f"[eval-config] eval_mix={eval_mix}  compression_ratio={compression_ratio}", flush=True)

    t0 = time.perf_counter()
    print(f"[load-data] reading CSV: {args.csv_path}", flush=True)
    trajectories, trajectory_mmsis = load_ais_csv(args.csv_path, return_mmsis=True)
    if args.max_trajectories is not None and len(trajectories) > args.max_trajectories:
        trajectories = trajectories[: args.max_trajectories]
        trajectory_mmsis = trajectory_mmsis[: args.max_trajectories]
        print(f"[load-data] capped trajectories to {args.max_trajectories}", flush=True)
    print(f"[load-data] {len(trajectories)} trajectories in {time.perf_counter() - t0:.2f}s", flush=True)

    dataset = TrajectoryDataset(trajectories)
    points = dataset.get_all_points()
    boundaries = dataset.get_trajectory_boundaries()
    print(f"[dataset] points={points.shape[0]}  trajectories={len(boundaries)}", flush=True)

    t0 = time.perf_counter()
    if args.queries_from:
        print(f"[workload] loading saved queries from {args.queries_from}", flush=True)
        typed = read_queries_geojson(args.queries_from)
        if not typed:
            raise RuntimeError(f"No queries found under {args.queries_from}")
        features, type_ids = pad_query_features(typed)
        covered = query_coverage_mask(points, typed)
        covered_points = int(covered.sum().item())
        total_points = int(points.shape[0])
        workload = TypedQueryWorkload(
            query_features=features,
            typed_queries=typed,
            type_ids=type_ids,
            coverage_fraction=float(covered_points / total_points) if total_points > 0 else 0.0,
            covered_points=covered_points,
            total_points=total_points,
        )
    else:
        workload = generate_typed_query_workload(
            trajectories=trajectories,
            n_queries=int(args.n_queries),
            workload_mix=eval_mix,
            seed=int(args.seed),
            target_coverage=args.query_coverage,
            max_queries=args.max_queries,
            range_spatial_fraction=args.range_spatial_fraction,
            range_time_fraction=args.range_time_fraction,
            knn_k=args.knn_k,
            similarity_time_fraction=args.similarity_time_fraction,
            similarity_top_k=args.similarity_top_k,
        )
    coverage_msg = ""
    if workload.coverage_fraction is not None:
        coverage_msg = (
            f"  coverage={100.0 * workload.coverage_fraction:.2f}% "
            f"({workload.covered_points}/{workload.total_points})"
        )
    print(
        f"[workload] generated {len(workload.typed_queries)} queries in "
        f"{time.perf_counter() - t0:.2f}s{coverage_msg}",
        flush=True,
    )

    if args.save_queries_dir:
        write_queries_geojson(args.save_queries_dir, workload.typed_queries)
        print(f"[workload] wrote queries GeoJSON to {args.save_queries_dir}", flush=True)

    # Adapt ModelArtifacts -> TrainingOutputs (MLQDSMethod only reads .model and .scaler).
    trained = TrainingOutputs(
        model=artifacts.model,
        scaler=artifacts.scaler,
        labels=torch.zeros((1, NUM_QUERY_TYPES), dtype=torch.float32),
        labelled_mask=torch.zeros((1,), dtype=torch.bool),
        history=[],
        epochs_trained=int(artifacts.epochs_trained),
    )

    if args.no_query_model:
        # Empty workload for the model: query_features has shape (0, F) where
        # F is the per-query feature width used during training. Inferring F
        # from the generated workload keeps the tensor compatible with the
        # model's cross-attention input projection.
        empty_features = workload.query_features.new_zeros((0, workload.query_features.shape[1]))
        model_workload = TypedQueryWorkload(
            query_features=empty_features,
            typed_queries=[],
            type_ids=torch.zeros((0,), dtype=torch.long),
        )
        print("[no-query-model] MLQDS will receive an EMPTY workload — eval queries are unchanged.", flush=True)
    else:
        model_workload = workload

    methods = [
        MLQDSMethod(
            name="MLQDS",
            trained=trained,
            workload=model_workload,
            workload_mix=eval_mix,
            temporal_fraction=float(getattr(saved_cfg.model, "mlqds_temporal_fraction", 0.50)),
            diversity_bonus=float(getattr(saved_cfg.model, "mlqds_diversity_bonus", 0.05)),
            simplification_mode=str(getattr(saved_cfg.model, "simplification_mode", "score_coverage")),
            coverage_lambda=float(getattr(saved_cfg.model, "coverage_lambda", 0.5)),
            coverage_sigma_fraction=float(getattr(saved_cfg.model, "coverage_sigma_fraction", 0.5)),
            length_preservation_weight=float(getattr(saved_cfg.model, "length_preservation_weight", 0.0)),
        ),
        NewUniformTemporalMethod(),
        DouglasPeuckerMethod(),
    ]

    results: dict[str, Any] = {}
    # Capture masks for ALL methods so Scope-2 / Scope-3 length retention can
    # be computed per method below (not just MLQDS).
    save_masks = True
    # Cache full_res + support_masks once — shared across all methods.
    from src.evaluation.evaluate_methods import precompute_query_cache
    print("[eval] precomputing query cache ...", flush=True)
    t_cache = time.perf_counter()
    eval_cache = precompute_query_cache(
        points=points,
        boundaries=boundaries,
        typed_queries=workload.typed_queries,
    )
    print(f"[eval] query cache built in {time.perf_counter() - t_cache:.2f}s", flush=True)
    for method in methods:
        t0 = time.perf_counter()
        print(f"[eval] {method.name} ...", flush=True)
        results[method.name] = evaluate_method(
            method=method,
            points=points,
            boundaries=boundaries,
            typed_queries=workload.typed_queries,
            workload_mix=eval_mix,
            compression_ratio=compression_ratio,
            return_mask=True,
            cache=eval_cache,
        )
        print(f"[eval] {method.name} done in {time.perf_counter() - t0:.2f}s", flush=True)

    # ---- Scope-3 (in-query) length retention per method ----
    # The matched-eval pipeline computes Scope-3 during training diagnostics,
    # but run_inference only printed Scope-1. Recompute it here from each
    # method's retained mask + per-query support masks so re-eval runs report
    # the same Scope-3 numbers as the training logs.
    from src.evaluation.evaluate_methods import (
        _query_support_mask,
        _split_by_boundaries,
        print_length_retention_in_query_table,
        print_length_retention_in_query_by_length_table,
    )
    from src.evaluation.metrics import (
        compute_in_query_length_retention,
        compute_in_query_length_retention_bucketed,
        compute_full_trajectory_lengths_km,
        count_trajectories_by_length_bucket,
        SCOPE3_LENGTH_BUCKETS,
    )
    print("[eval] computing Scope-3 in-query retention ...", flush=True)
    t_s3 = time.perf_counter()
    full_traj_s3 = _split_by_boundaries(points, boundaries)
    full_traj_km = compute_full_trajectory_lengths_km(points, boundaries)
    bucket_order = [label for _, _, label in SCOPE3_LENGTH_BUCKETS]
    traj_counts_by_bucket = count_trajectories_by_length_bucket(full_traj_km)
    method_names_s3 = [m.name for m in methods]
    s3_aggs: dict[str, dict[str, float]] = {
        n: {"sum_orig_km": 0.0, "sum_simp_km": 0.0, "n_segments": 0} for n in method_names_s3
    }
    # Per-method, per-length-bucket running sums for the Scope-3 sub-scope.
    s3_bucketed: dict[str, dict[str, dict[str, float]]] = {
        n: {label: {"sum_orig_km": 0.0, "sum_simp_km": 0.0, "n_segments": 0} for label in bucket_order}
        for n in method_names_s3
    }
    for q_idx, query in enumerate(workload.typed_queries):
        # Reuse cached support for non-range types; recompute range cheaply.
        support = eval_cache.support[q_idx]
        if query["type"] == "range" or int(support.sum().item()) == 0:
            support = _query_support_mask(points, boundaries, query, full_traj_s3)
        if int(support.sum().item()) == 0:
            continue
        for name in method_names_s3:
            mask = results[name].retained_mask
            if mask is None:
                continue
            iq = compute_in_query_length_retention(points, boundaries, mask, support)
            n_seg = int(iq["n_segments"])
            if n_seg > 0:
                s3_aggs[name]["sum_orig_km"] += float(iq["avg_orig_km"]) * n_seg
                s3_aggs[name]["sum_simp_km"] += float(iq["avg_simp_km"]) * n_seg
                s3_aggs[name]["n_segments"] += n_seg
            # Sub-scope: bucket the same segments by parent-trajectory length.
            bkt = compute_in_query_length_retention_bucketed(
                points, boundaries, mask, support, full_traj_km
            )
            for label, agg in bkt.items():
                s3_bucketed[name][label]["sum_orig_km"] += agg["sum_orig_km"]
                s3_bucketed[name][label]["sum_simp_km"] += agg["sum_simp_km"]
                s3_bucketed[name][label]["n_segments"] += agg["n_segments"]
    length_retention_in_query_table = print_length_retention_in_query_table(s3_aggs)
    length_retention_in_query_by_length_table = print_length_retention_in_query_by_length_table(
        s3_bucketed, bucket_order, traj_counts=traj_counts_by_bucket
    )
    print(f"[eval] Scope-3 computed in {time.perf_counter() - t_s3:.2f}s", flush=True)

    mlqds_mask = results["MLQDS"].retained_mask
    if mlqds_mask is None:
        raise RuntimeError("MLQDS retained mask was not captured during inference evaluation.")

    length_retention_whole_set_table = print_length_retention_whole_set_table(results, points, boundaries)
    out_dir = Path(args.results_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.no_query_model:
        # No-query generalization mode: the model scored points WITHOUT seeing
        # queries, but eval F1 is still computed against the real eval queries.
        # The matched-workload table's "all" row already carries the aggregate
        # AnswerF1, so the separate whole-dataset table is redundant and omitted.
        matched_table = print_method_comparison_table(results)
        print("\nMatched-workload AnswerF1 table (no-query model; per-type breakdown + Diff vs DP)")
        print(matched_table)
        print("\n" + length_retention_whole_set_table)
        print("\n" + length_retention_in_query_table)
        print("\n" + length_retention_in_query_by_length_table)
        (out_dir / "matched_table.txt").write_text(matched_table + "\n", encoding="utf-8")
        (out_dir / "length_retention_whole_set_table.txt").write_text(length_retention_whole_set_table + "\n", encoding="utf-8")
        (out_dir / "length_retention_in_query_table.txt").write_text(length_retention_in_query_table + "\n", encoding="utf-8")
        (out_dir / "length_retention_in_query_by_length_table.txt").write_text(length_retention_in_query_by_length_table + "\n", encoding="utf-8")
        geometric_table = ""
    else:
        table = print_method_comparison_table(results)
        geometric_table = print_geometric_distortion_table(results)
        print("\nMatched-workload table (inference on new CSV)")
        print(table)
        print("\nGeometric-distortion table (lower is better; SED = time-synchronous, PED = perpendicular, in km)")
        print(geometric_table)
        print("\n" + length_retention_whole_set_table)
        print("\n" + length_retention_in_query_table)
        print("\n" + length_retention_in_query_by_length_table)
        (out_dir / "matched_table.txt").write_text(table + "\n", encoding="utf-8")
        (out_dir / "geometric_distortion_table.txt").write_text(geometric_table + "\n", encoding="utf-8")
        (out_dir / "length_retention_whole_set_table.txt").write_text(length_retention_whole_set_table + "\n", encoding="utf-8")
        (out_dir / "length_retention_in_query_table.txt").write_text(length_retention_in_query_table + "\n", encoding="utf-8")
        (out_dir / "length_retention_in_query_by_length_table.txt").write_text(length_retention_in_query_by_length_table + "\n", encoding="utf-8")

    dump = {
        "checkpoint": str(args.checkpoint),
        "csv_path": str(args.csv_path),
        "n_trajectories": len(trajectories),
        "n_points": int(points.shape[0]),
        "eval_mix": eval_mix,
        "compression_ratio": compression_ratio,
        "query_coverage": workload.coverage_fraction,
        "covered_points": workload.covered_points,
        "total_points": workload.total_points,
        "matched": {
            name: {
                "aggregate_f1": m.aggregate_f1,
                "per_type_f1": m.per_type_f1,
                "compression_ratio": m.compression_ratio,
                "latency_ms": m.latency_ms,
                "avg_retained_point_gap": m.avg_retained_point_gap,
                "avg_retained_point_gap_norm": m.avg_retained_point_gap_norm,
                "max_retained_point_gap": m.max_retained_point_gap,
                "geometric_distortion": m.geometric_distortion,
                "avg_length_preserved": m.avg_length_preserved,
                "length_preservation_aggregates": m.length_preservation_aggregates,
                "combined_query_shape_score": m.combined_query_shape_score,
            }
            for name, m in results.items()
        },
    }
    with open(out_dir / "inference_run.json", "w", encoding="utf-8") as f:
        json.dump(dump, f, indent=2)
    print(f"[write] results -> {out_dir}", flush=True)


    if args.save_simplified_dir:
        out_dir_simp = Path(args.save_simplified_dir)
        out_dir_simp.mkdir(parents=True, exist_ok=True)
        write_simplified_csv(str(out_dir_simp / "ML_simplified.csv"), points, boundaries, mlqds_mask, trajectory_mmsis=trajectory_mmsis)
        print(f"[write] simplified CSV -> {out_dir_simp / 'ML_simplified.csv'}", flush=True)
        for ref_name, csv_name in (("uniform", "uniform_simplified.csv"),
                                   ("DouglasPeucker", "DP_simplified.csv")):
            ref_eval = results.get(ref_name)
            if ref_eval is not None and ref_eval.retained_mask is not None:
                write_simplified_csv(
                    str(out_dir_simp / csv_name),
                    points,
                    boundaries,
                    ref_eval.retained_mask,
                    trajectory_mmsis=trajectory_mmsis,
                )
                print(f"[write] simplified CSV -> {out_dir_simp / csv_name}", flush=True)


if __name__ == "__main__":
    main()
