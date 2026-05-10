"""Experiment orchestration helpers for training and evaluation runs. See src/experiments/README.md for details."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import torch


@contextmanager
def _phase(name: str):
    """Log a named phase with wall-clock timing."""
    print(f"[{name}] starting...", flush=True)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        print(f"[{name}] done in {dt:.2f}s", flush=True)

from src.data.trajectory_dataset import TrajectoryDataset
from src.evaluation.baselines import (
    DouglasPeuckerMethod,
    MLQDSMethod,
    NewUniformTemporalMethod,
    OracleMethod,
)
from src.evaluation.evaluate_methods import (
    evaluate_method,
    build_query_support_masks,
    compute_eval_query_length_preservation,
    compute_per_query_diagnostics,
    print_geometric_distortion_table,
    print_length_preservation_table,
    print_method_comparison_table,
    print_top_queries_table,
    write_top_queries_csv,
    print_shift_table,
)
from src.experiments.experiment_config import ExperimentConfig, TypedQueryWorkload, derive_seed_bundle
from src.experiments.geojson_writers import (
    report_trajectory_length_loss,
    write_queries_geojson,
    write_query_points_csv,
    write_simplified_csv,
)
from src.queries.query_generator import generate_typed_query_workload
from src.queries.query_types import parse_workload_mix
from src.training.importance_labels import compute_typed_importance_labels
from src.training.train_model import train_model
from src.training.training_pipeline import ModelArtifacts, save_checkpoint


@dataclass
class ExperimentOutputs:
    """Experiment run output payload. See src/experiments/README.md for details."""

    matched_table: str
    shift_table: str
    metrics_dump: dict
    geometric_table: str = ""
    length_preservation_table: str = ""
    eval_query_length_table: str = ""
    top_best_queries_table: str = ""
    top_worst_queries_table: str = ""


def split_trajectories(
    trajectories: list[torch.Tensor],
    train_fraction: float,
    val_fraction: float,
    seed: int,
) -> tuple[list[torch.Tensor], list[torch.Tensor], list[torch.Tensor]]:
    """Deterministically split trajectories at trajectory level. See src/experiments/README.md for details."""
    n = len(trajectories)
    g = torch.Generator().manual_seed(int(seed))
    perm = torch.randperm(n, generator=g).tolist()

    n_train = max(1, int(n * train_fraction))
    n_val = max(1, int(n * val_fraction)) if n - n_train > 1 else 0
    n_test = max(1, n - n_train - n_val)
    if n_train + n_val + n_test > n:
        n_test = n - n_train - n_val

    train = [trajectories[i] for i in perm[:n_train]]
    val = [trajectories[i] for i in perm[n_train : n_train + n_val]]
    test = [trajectories[i] for i in perm[n_train + n_val :]]
    if not test:
        test = val if val else train
    return train, val, test


def _mix_name(mix: dict[str, float]) -> str:
    """Build compact string name for workload mix maps. See src/experiments/README.md for details."""
    return ",".join(f"{k}={v:.1f}" for k, v in sorted(mix.items()))


def _coverage_name(workload: TypedQueryWorkload) -> str:
    """Format workload point-coverage metadata for logs."""
    if workload.coverage_fraction is None:
        return "unknown"
    covered = workload.covered_points if workload.covered_points is not None else 0
    total = workload.total_points if workload.total_points is not None else 0
    return f"{100.0 * workload.coverage_fraction:.2f}% ({covered}/{total})"


def _normalized_coverage_target(value: float | None) -> float | None:
    """Normalize coverage target for pipeline warnings."""
    if value is None:
        return None
    target = float(value)
    return target / 100.0 if target > 1.0 else target


def _validation_query_count(config: ExperimentConfig) -> int:
    """Use a broader validation workload for less seed-specific F1 checkpointing."""
    doubled = max(int(config.query.n_queries), int(config.query.n_queries) * 2)
    if config.query.max_queries is None:
        return doubled
    return min(doubled, max(int(config.query.n_queries), int(config.query.max_queries)))


def run_experiment_pipeline(
    config: ExperimentConfig,
    trajectories: list[torch.Tensor],
    train_mix: dict[str, float],
    eval_mix: dict[str, float],
    results_dir: str,
    save_model: str | None = None,
    save_queries_dir: str | None = None,
    save_simplified_dir: str | None = None,
    trajectory_mmsis: list[int] | None = None,
    eval_trajectories: list[torch.Tensor] | None = None,
    eval_trajectory_mmsis: list[int] | None = None,
) -> ExperimentOutputs:
    """Run training, matched evaluation, and shifted evaluation tables. See src/experiments/README.md for details."""
    pipeline_t0 = time.perf_counter()
    if eval_trajectories is None:
        print(
            f"[pipeline] {len(trajectories)} trajectories, train_mix={_mix_name(train_mix)}, "
            f"eval_mix={_mix_name(eval_mix)}",
            flush=True,
        )
    else:
        print(
            f"[pipeline] train={len(trajectories)} trajectories, eval={len(eval_trajectories)} trajectories, "
            f"train_mix={_mix_name(train_mix)}, eval_mix={_mix_name(eval_mix)}",
            flush=True,
        )

    seeds = derive_seed_bundle(config.data.seed)
    selection_metric = str(getattr(config.model, "checkpoint_selection_metric", "loss")).lower()
    f1_diag_every = int(getattr(config.model, "f1_diagnostic_every", 0) or 0)
    needs_validation_f1 = selection_metric in {"f1", "uniform_gap"} or f1_diag_every > 0
    with _phase("split"):
        selection_traj: list[torch.Tensor] | None = None
        if eval_trajectories is None:
            # Reproduce split_trajectories' permutation here so we can align the
            # MMSI list with the test split (the helper itself doesn't carry ids).
            n = len(trajectories)
            g = torch.Generator().manual_seed(int(seeds.split_seed))
            perm = torch.randperm(n, generator=g).tolist()
            n_train = max(1, int(n * config.data.train_fraction))
            n_val = max(1, int(n * config.data.val_fraction)) if n - n_train > 1 else 0
            train_traj = [trajectories[i] for i in perm[:n_train]]
            _val_traj = [trajectories[i] for i in perm[n_train : n_train + n_val]]
            test_traj = [trajectories[i] for i in perm[n_train + n_val :]]
            if not test_traj:
                test_traj = _val_traj if _val_traj else train_traj
            selection_traj = _val_traj if needs_validation_f1 and _val_traj else None
            if trajectory_mmsis is not None and len(trajectory_mmsis) == n:
                train_mmsis = [trajectory_mmsis[i] for i in perm[:n_train]]
                test_mmsis = [trajectory_mmsis[i] for i in perm[n_train + n_val :]]
                if not test_mmsis:
                    test_mmsis = [trajectory_mmsis[i] for i in perm[n_train : n_train + n_val]] or \
                                 [trajectory_mmsis[i] for i in perm[:n_train]]
            else:
                train_mmsis = None
                test_mmsis = None
            print(f"  split mode=single dataset  train={len(train_traj)}  test={len(test_traj)}", flush=True)
        else:
            train_traj = trajectories
            test_traj = eval_trajectories
            train_mmsis = trajectory_mmsis
            test_mmsis = eval_trajectory_mmsis
            if needs_validation_f1:
                n = len(train_traj)
                g = torch.Generator().manual_seed(int(seeds.split_seed))
                perm = torch.randperm(n, generator=g).tolist()
                n_val = max(1, int(n * config.data.val_fraction)) if n > 1 else 0
                val_idx = set(perm[:n_val])
                selection_traj = [traj for idx, traj in enumerate(train_traj) if idx in val_idx]
                train_traj = [traj for idx, traj in enumerate(train_traj) if idx not in val_idx]
                if train_mmsis is not None and len(train_mmsis) == n:
                    train_mmsis = [mmsi for idx, mmsi in enumerate(train_mmsis) if idx not in val_idx]
            print(f"  split mode=separate CSVs  train={len(train_traj)}  eval={len(test_traj)}", flush=True)
        if selection_traj:
            print(f"  checkpoint-selection validation={len(selection_traj)} trajectories", flush=True)

    with _phase("build-datasets"):
        train_ds = TrajectoryDataset(train_traj)
        test_ds = TrajectoryDataset(test_traj)
        selection_ds = TrajectoryDataset(selection_traj) if selection_traj else None
        train_points = train_ds.get_all_points()
        test_points = test_ds.get_all_points()
        train_boundaries = train_ds.get_trajectory_boundaries()
        test_boundaries = test_ds.get_trajectory_boundaries()
        selection_boundaries = selection_ds.get_trajectory_boundaries() if selection_ds is not None else None

    with _phase("generate-workloads"):
        # Front-load all kNN queries before proportional scheduling for training
        # so kNN always gets its full quota even if n_queries is small.
        knn_front_load = int(train_mix.get("knn", 0.0) * config.query.n_queries)
        train_workload = generate_typed_query_workload(
            trajectories=train_traj,
            n_queries=config.query.n_queries,
            workload_mix=train_mix,
            seed=seeds.train_query_seed,
            target_coverage=config.query.target_coverage,
            max_queries=config.query.max_queries,
            range_spatial_fraction=config.query.range_spatial_fraction,
            range_time_fraction=config.query.range_time_fraction,
            knn_k=config.query.knn_k,
            knn_t_half_window_fraction=config.query.knn_t_half_window_fraction,
            similarity_time_fraction=config.query.similarity_time_fraction,
            front_load_knn=knn_front_load,
        )
        eval_workload = generate_typed_query_workload(
            trajectories=test_traj,
            n_queries=config.query.n_queries,
            workload_mix=eval_mix,
            seed=seeds.eval_query_seed,
            target_coverage=config.query.target_coverage,
            max_queries=config.query.max_queries,
            range_spatial_fraction=config.query.range_spatial_fraction,
            range_time_fraction=config.query.range_time_fraction,
            knn_k=config.query.knn_k,
            knn_t_half_window_fraction=config.query.knn_t_half_window_fraction,
            similarity_time_fraction=config.query.similarity_time_fraction,
        )
        selection_workload = None
        if selection_traj:
            selection_workload = generate_typed_query_workload(
                trajectories=selection_traj,
                n_queries=_validation_query_count(config),
                workload_mix=eval_mix,
                seed=seeds.eval_query_seed + 17,
                target_coverage=config.query.target_coverage,
                max_queries=config.query.max_queries,
                range_spatial_fraction=config.query.range_spatial_fraction,
                range_time_fraction=config.query.range_time_fraction,
                knn_k=config.query.knn_k,
            )
        print(
            f"  train_workload={len(train_workload.typed_queries)} queries  "
            f"coverage={_coverage_name(train_workload)}",
            flush=True,
        )
        print(
            f"  eval_workload={len(eval_workload.typed_queries)} queries  "
            f"coverage={_coverage_name(eval_workload)}",
            flush=True,
        )
        if selection_workload is not None:
            print(
                f"  selection_workload={len(selection_workload.typed_queries)} queries  "
                f"coverage={_coverage_name(selection_workload)}",
                flush=True,
            )
        target = _normalized_coverage_target(config.query.target_coverage)
        if target is not None:
            for label, workload in (("train", train_workload), ("eval", eval_workload)):
                coverage = float(workload.coverage_fraction or 0.0)
                if coverage + 1e-9 < target:
                    print(
                        f"  WARNING: {label} workload stopped below requested coverage "
                        f"({coverage:.2%} < {target:.2%}); raise --n_queries or query footprint to cover more points.",
                        flush=True,
                    )

    if save_queries_dir:
        with _phase("write-queries-geojson"):
            write_queries_geojson(save_queries_dir, eval_workload.typed_queries)

    with _phase(f"train-model ({config.model.epochs} epochs)"):
        trained = train_model(
            train_trajectories=train_traj,
            train_boundaries=train_boundaries,
            workload=train_workload,
            model_config=config.model,
            seed=seeds.torch_seed,
            train_mix=train_mix,
            validation_trajectories=selection_traj,
            validation_boundaries=selection_boundaries,
            validation_workload=selection_workload,
            validation_mix=eval_mix if selection_workload is not None else None,
        )

    if save_model:
        with _phase("save-model"):
            artifacts = ModelArtifacts(
                model=trained.model,
                scaler=trained.scaler,
                config=config,
                epochs_trained=trained.epochs_trained,
                train_workload_mix=train_mix,
                eval_workload_mix=eval_mix,
            )
            save_checkpoint(save_model, artifacts)
            print(
                f"  saved checkpoint to {save_model}  "
                f"(epochs_trained={trained.epochs_trained}, "
                f"best_epoch={trained.best_epoch}, best_loss={trained.best_loss:.8f}, "
                f"train_mix={_mix_name(train_mix)}, eval_mix={_mix_name(eval_mix)})",
                flush=True,
            )
    methods = [
        MLQDSMethod(
            name="MLQDS",
            trained=trained,
            workload=eval_workload,
            workload_mix=eval_mix,
            temporal_fraction=config.model.mlqds_temporal_fraction,
            diversity_bonus=config.model.mlqds_diversity_bonus,
            simplification_mode=getattr(config.model, "simplification_mode", "score_coverage"),
            coverage_lambda=float(getattr(config.model, "coverage_lambda", 0.5)),
            coverage_sigma_fraction=float(getattr(config.model, "coverage_sigma_fraction", 0.5)),
        ),
        NewUniformTemporalMethod(),
        DouglasPeuckerMethod(),
    ]

    matched: dict[str, Any] = {}
    save_masks = bool(save_simplified_dir)
    with _phase("evaluate-matched"):
        for method in methods:
            with _phase(f"  eval {method.name}"):
                matched[method.name] = evaluate_method(
                    method=method,
                    points=test_points,
                    boundaries=test_boundaries,
                    typed_queries=eval_workload.typed_queries,
                    workload_mix=eval_mix,
                    compression_ratio=config.model.compression_ratio,
                    return_mask=method.name in {"MLQDS", "uniform", "DouglasPeucker"} or save_masks,
                )

        eval_labels, _ = compute_typed_importance_labels(
            points=test_points,
            boundaries=test_boundaries,
            typed_queries=eval_workload.typed_queries,
            seed=seeds.eval_query_seed,
            knn_label_variant=str(getattr(config.model, "knn_label_variant", "legacy")),
            range_label_variant=str(getattr(config.model, "range_label_variant", "legacy")),
        )
        oracle = OracleMethod(labels=eval_labels, workload_mix=eval_mix)
        with _phase(f"  eval {oracle.name}"):
            matched[oracle.name] = evaluate_method(
                method=oracle,
                points=test_points,
                boundaries=test_boundaries,
                typed_queries=eval_workload.typed_queries,
                workload_mix=eval_mix,
                compression_ratio=config.model.compression_ratio,
            )

    matched_table = print_method_comparison_table(matched)
    geometric_table = print_geometric_distortion_table(matched)
    length_preservation_table = print_length_preservation_table(matched)

    diag_methods = ["MLQDS", "uniform", "DouglasPeucker"]
    masks_for_diag: dict[str, Any] = {}
    for name in diag_methods:
        m = matched.get(name)
        if m is not None and m.retained_mask is not None:
            masks_for_diag[name] = m.retained_mask
    eval_query_length_pres = compute_eval_query_length_preservation(
        test_points, test_boundaries, masks_for_diag, eval_workload.typed_queries,
    )
    per_query_diagnostics = compute_per_query_diagnostics(
        test_points, test_boundaries, masks_for_diag, eval_workload.typed_queries,
    )
    top_best_table = print_top_queries_table(per_query_diagnostics, k=15, mode="best")
    top_worst_table = print_top_queries_table(per_query_diagnostics, k=15, mode="worst")
    eval_query_lp_lines = ["Eval-query-points length preservation (trajectories any eval query touches)",
                           f"{'Method':<24}{'Weighted':>11}{'orig_km':>12}{'simp_km':>12}{'NTraj':>8}",
                           "-" * 67]
    for name, agg in eval_query_length_pres.items():
        eval_query_lp_lines.append(
            f"{name:<24}{agg.get('weighted_ratio', 0.0):>11.4f}{agg.get('sum_orig_km', 0.0):>12.2f}"
            f"{agg.get('sum_simp_km', 0.0):>12.2f}{int(agg.get('n_trajectories', 0)):>8d}"
        )
    eval_query_length_table = "\n".join(eval_query_lp_lines)

    with _phase("evaluate-shift"):
        train_name = _mix_name(train_mix)
        eval_name = _mix_name(eval_mix)
        shift_pairs = {train_name: {eval_name: float(matched["MLQDS"].aggregate_f1)}}
        if train_name == eval_name:
            shift_pairs[train_name][train_name] = float(matched["MLQDS"].aggregate_f1)
        else:
            shift_pairs[train_name][train_name] = float(
                evaluate_method(
                    method=MLQDSMethod(
                        name="MLQDS",
                        trained=trained,
                        workload=train_workload,
                        workload_mix=train_mix,
                        temporal_fraction=config.model.mlqds_temporal_fraction,
                        diversity_bonus=config.model.mlqds_diversity_bonus,
                        simplification_mode=getattr(config.model, "simplification_mode", "score_coverage"),
                        coverage_lambda=float(getattr(config.model, "coverage_lambda", 0.5)),
                        coverage_sigma_fraction=float(getattr(config.model, "coverage_sigma_fraction", 0.5)),
                    ),
                    points=test_points,
                    boundaries=test_boundaries,
                    typed_queries=train_workload.typed_queries,
                    workload_mix=train_mix,
                    compression_ratio=config.model.compression_ratio,
                ).aggregate_f1
            )
    shift_table = print_shift_table(shift_pairs)

    dump = {
        "config": config.to_dict(),
        "train_mix": train_mix,
        "eval_mix": eval_mix,
        "train_query_count": len(train_workload.typed_queries),
        "eval_query_count": len(eval_workload.typed_queries),
        "train_query_coverage": train_workload.coverage_fraction,
        "eval_query_coverage": eval_workload.coverage_fraction,
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
            for name, m in matched.items()
        },
        "shift": shift_pairs,
        "eval_query_length_preservation": eval_query_length_pres,
        "per_query_diagnostics": per_query_diagnostics,
        "training_history": trained.history,
        "best_epoch": trained.best_epoch,
        "best_loss": trained.best_loss,
        "best_f1": trained.best_f1,
    }

    with _phase("write-results"):
        out_dir = Path(results_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "matched_table.txt").write_text(matched_table + "\n", encoding="utf-8")
        (out_dir / "shift_table.txt").write_text(shift_table + "\n", encoding="utf-8")
        (out_dir / "geometric_distortion_table.txt").write_text(geometric_table + "\n", encoding="utf-8")
        (out_dir / "length_preservation_table.txt").write_text(length_preservation_table + "\n", encoding="utf-8")
        (out_dir / "eval_query_length_table.txt").write_text(eval_query_length_table + "\n", encoding="utf-8")
        (out_dir / "top_best_queries_table.txt").write_text(top_best_table + "\n", encoding="utf-8")
        (out_dir / "top_worst_queries_table.txt").write_text(top_worst_table + "\n", encoding="utf-8")
        write_top_queries_csv(per_query_diagnostics, str(out_dir / "top_best_queries.csv"), k=15, mode="best")
        write_top_queries_csv(per_query_diagnostics, str(out_dir / "top_worst_queries.csv"), k=15, mode="worst")
        with open(out_dir / "example_run.json", "w", encoding="utf-8") as f:
            json.dump(dump, f, indent=2)
        print(f"  wrote results to {out_dir}", flush=True)

    if save_simplified_dir:
        with _phase("write-simplified-csv"):
            out_dir = Path(save_simplified_dir)
            eval_mask = matched["MLQDS"].retained_mask
            if eval_mask is None:
                eval_mlqds = MLQDSMethod(
                    name="MLQDS",
                    trained=trained,
                    workload=eval_workload,
                    workload_mix=eval_mix,
                    temporal_fraction=config.model.mlqds_temporal_fraction,
                    diversity_bonus=config.model.mlqds_diversity_bonus,
                    simplification_mode=getattr(config.model, "simplification_mode", "score_coverage"),
                    coverage_lambda=float(getattr(config.model, "coverage_lambda", 0.5)),
                    coverage_sigma_fraction=float(getattr(config.model, "coverage_sigma_fraction", 0.5)),
                )
                eval_mask = eval_mlqds.simplify(test_points, test_boundaries, config.model.compression_ratio)
            write_simplified_csv(
                str(out_dir / "ML_simplified_eval.csv"),
                test_points,
                test_boundaries,
                eval_mask,
                trajectory_mmsis=test_mmsis,
            )
            for ref_name, csv_name in (("uniform", "uniform_simplified_eval.csv"),
                                       ("DouglasPeucker", "DP_simplified_eval.csv")):
                ref_mask = matched.get(ref_name).retained_mask if matched.get(ref_name) is not None else None
                if ref_mask is not None:
                    write_simplified_csv(
                        str(out_dir / csv_name),
                        test_points,
                        test_boundaries,
                        ref_mask,
                        trajectory_mmsis=test_mmsis,
                    )

        with _phase("write-query-points-csv"):
            support_masks = build_query_support_masks(test_points, test_boundaries, eval_workload.typed_queries)
            write_query_points_csv(
                str(out_dir),
                test_points,
                test_boundaries,
                eval_workload.typed_queries,
                support_masks,
                masks_for_diag,
                trajectory_mmsis=test_mmsis,
            )

        with _phase("trajectory-length-loss"):
            report_trajectory_length_loss(test_points, test_boundaries, eval_mask, top_k=25, trajectory_mmsis=test_mmsis)

    print(f"[pipeline] total runtime {time.perf_counter() - pipeline_t0:.2f}s", flush=True)
    return ExperimentOutputs(
        matched_table=matched_table,
        shift_table=shift_table,
        metrics_dump=dump,
        geometric_table=geometric_table,
        length_preservation_table=length_preservation_table,
        eval_query_length_table=eval_query_length_table,
        top_best_queries_table=top_best_table,
        top_worst_queries_table=top_worst_table,
    )


def _workload_keyword_to_mix(keyword: str | None) -> dict[str, float] | None:
    """Translate a --workload keyword to a concrete mix, or return None.

    - "mixed"        -> all 4 types (range-heavy: 0.4/0.2/0.2/0.2).
    - "local_mixed"  -> local/point-based types (range=0.6, knn=0.4).
                        These use small boxes / neighbourhood lookups, cheap to eval.
    - "global_mixed" -> trajectory-global types (similarity=0.5, clustering=0.5).
                        DBSCAN + DTW; expensive, needs long wall time.
    - "range"/"knn"/"similarity"/"clustering" -> 100% that type.
    - anything else -> None (fall back to caller default).
    """
    if not keyword:
        return None
    k = keyword.strip().lower()
    if k == "mixed":
        return {"range": 0.4, "knn": 0.2, "similarity": 0.2, "clustering": 0.2}
    if k == "local_mixed":
        return {"range": 0.6, "knn": 0.4}
    if k == "global_mixed":
        return {"similarity": 0.5, "clustering": 0.5}
    if k in {"range", "knn", "similarity", "clustering"}:
        return {k: 1.0}
    return None


def resolve_workload_mixes(
    train_workload_mix_arg: str | None,
    eval_workload_mix_arg: str | None,
    workload_keyword: str | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    """Parse and normalize train/eval workload mix strings. See src/experiments/README.md for details.

    Priority: explicit --train_workload_mix / --eval_workload_mix strings win.
    Otherwise, if --workload is a recognised keyword, both mixes follow it.
    Otherwise, fall back to the historical mixed-shift defaults.
    """
    keyword_mix = _workload_keyword_to_mix(workload_keyword)
    if keyword_mix is not None:
        default_train = keyword_mix
        default_eval = keyword_mix
    else:
        default_train = {"range": 0.8, "knn": 0.2}
        default_eval = {"range": 0.2, "clustering": 0.8}
    train_mix = parse_workload_mix(train_workload_mix_arg, default=default_train)
    eval_mix = parse_workload_mix(eval_workload_mix_arg, default=default_eval)
    return train_mix, eval_mix
