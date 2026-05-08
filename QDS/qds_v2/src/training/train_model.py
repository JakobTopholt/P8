"""Ranking-based model training on trajectory windows. See src/training/README.md for details."""

from __future__ import annotations

import time
from dataclasses import dataclass
import math

import torch
import torch.nn.functional as F

from src.experiments.experiment_config import ModelConfig, TypedQueryWorkload
from src.models.trajectory_qds_model import TrajectoryQDSModel
from src.models.turn_aware_qds_model import TurnAwareQDSModel
from src.queries.query_types import ID_TO_QUERY_NAME, NUM_QUERY_TYPES
from src.simplification.simplify_trajectories import (
    evenly_spaced_indices,
    simplify_with_score_and_coverage,
    simplify_with_scores,
    simplify_with_temporal_score_hybrid,
)
from src.training.importance_labels import compute_typed_importance_labels
from src.training.scaler import FeatureScaler
from src.training.trajectory_batching import TrajectoryBatch, batch_windows, build_trajectory_windows

KENDALL_TIE_THRESHOLD = 1e-4


_QUANTILE_SUBSAMPLE_CAP = 1_000_000  # torch.quantile errors past 2^24 on some builds.


def _safe_quantile(t: torch.Tensor, q: float | torch.Tensor) -> torch.Tensor:
    """Quantile that tolerates very large input tensors.

    For tensors larger than ~1M elements, torch.quantile can fail with
    ``input tensor is too large``. This helper subsamples uniformly to a
    1M-element view, which gives a sufficiently accurate quantile estimate
    for diagnostic logging and label-rescaling purposes.
    """
    if t.numel() <= _QUANTILE_SUBSAMPLE_CAP:
        return torch.quantile(t, q)
    if t.is_floating_point():
        flat = t.detach().reshape(-1)
    else:
        flat = t.reshape(-1)
    perm = torch.randperm(flat.numel(), device=flat.device)[:_QUANTILE_SUBSAMPLE_CAP]
    return torch.quantile(flat[perm], q)


def _scaled_training_targets(labels: torch.Tensor, labelled_mask: torch.Tensor) -> torch.Tensor:
    """Rescale sparse F1 labels per type for optimization while preserving rank order."""
    targets = labels.clone()
    for type_idx in range(NUM_QUERY_TYPES):
        positive = labelled_mask[:, type_idx] & (labels[:, type_idx] > 0)
        if not bool(positive.any().item()):
            continue
        scale = _safe_quantile(labels[positive, type_idx].detach(), 0.95).clamp(min=1e-6)
        targets[:, type_idx] = torch.clamp(labels[:, type_idx] / scale, 0.0, 1.0)
    return targets


def _apply_temporal_residual_labels(
    labels: torch.Tensor,
    labelled_mask: torch.Tensor,
    boundaries: list[tuple[int, int]],
    compression_ratio: float,
    temporal_fraction: float,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Drop supervision for points the temporal base already keeps."""
    residual_labels = labels.clone()
    residual_mask = labelled_mask.clone()
    base_mask = torch.zeros((labels.shape[0],), dtype=torch.bool, device=labels.device)
    base_fraction = min(1.0, max(0.0, float(temporal_fraction)))

    for start, end in boundaries:
        n = int(end - start)
        if n <= 0:
            continue
        k_total = min(n, max(2, int(math.ceil(float(compression_ratio) * n))))
        k_base = min(k_total, max(2, int(math.ceil(k_total * base_fraction))))
        base_idx = evenly_spaced_indices(n, k_base, labels.device)
        base_mask[start + base_idx] = True

    residual_labels[base_mask] = 0.0
    residual_mask[base_mask] = False
    return residual_labels, residual_mask


def _balanced_pointwise_loss(
    pred: torch.Tensor,
    target: torch.Tensor,
    valid_mask: torch.Tensor,
    generator: torch.Generator,
    negatives_per_positive: int = 3,
) -> torch.Tensor:
    """Compute balanced BCE on all positives plus a bounded random set of zero labels."""
    valid_idx = torch.where(valid_mask)[0]
    if valid_idx.numel() == 0:
        return pred.new_tensor(0.0)

    valid_target = target[valid_idx]
    positive_idx = valid_idx[valid_target > 0]
    if positive_idx.numel() == 0:
        return pred.new_tensor(0.0)

    zero_idx = valid_idx[valid_target <= 0]
    max_zero = int(positive_idx.numel() * max(1, negatives_per_positive))
    if zero_idx.numel() > max_zero:
        perm = torch.randperm(zero_idx.numel(), generator=generator)[:max_zero]
        zero_idx = zero_idx[perm.to(zero_idx.device)]

    pointwise_idx = torch.cat([positive_idx, zero_idx]) if zero_idx.numel() > 0 else positive_idx
    return F.binary_cross_entropy_with_logits(pred[pointwise_idx], target[pointwise_idx].clamp(0.0, 1.0))


def _discriminative_sample(
    pred: torch.Tensor,
    target: torch.Tensor,
    n_each: int,
    generator: torch.Generator,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Return top+bottom-quantile subsample for more reliable rank correlation. See src/training/README.md for details.

    Computing Kendall tau on all labelled points is O(N^2) and noisy when the
    label distribution has many near-tied pairs.  Restricting to the top and
    bottom quantiles focuses the statistic on the pairs the ranker is expected
    to separate, where the signal is strongest.
    """
    n = target.numel()
    if n <= 2 * n_each:
        return pred, target
    q = _safe_quantile(target, torch.tensor([0.25, 0.75], dtype=torch.float32, device=target.device))
    bot = torch.where(target <= q[0])[0]
    top = torch.where(target >= q[1])[0]
    perm_b = torch.randperm(bot.numel(), generator=generator)[:n_each]
    perm_t = torch.randperm(top.numel(), generator=generator)[:n_each]
    idx = torch.cat([bot[perm_b], top[perm_t]])
    return pred[idx], target[idx]


@dataclass
class TrainingOutputs:
    """Training artifact container. See src/training/README.md for details."""

    model: TrajectoryQDSModel
    scaler: FeatureScaler
    labels: torch.Tensor
    labelled_mask: torch.Tensor
    history: list[dict[str, float]]
    epochs_trained: int = 0
    best_epoch: int = 0
    best_loss: float = float("inf")
    best_f1: float = 0.0


def _sample_epoch_mix(alpha: list[float], generator: torch.Generator) -> torch.Tensor:
    """Sample epoch workload weights from a lightweight Dirichlet approximation. See src/training/README.md for details.

    A uniform floor of 0.5 * (1/K) is mixed in so no type ever receives
    near-zero training signal in a single epoch.  This keeps the per-type
    ranking heads from diverging due to gradient starvation.
    """
    a = torch.tensor(alpha, dtype=torch.float32)
    u = torch.rand((len(alpha),), generator=generator)
    raw = torch.pow(torch.clamp(u, min=1e-6), 1.0 / torch.clamp(a, min=1e-3))
    dirichlet = raw / raw.sum()
    uniform = torch.ones_like(dirichlet) / len(alpha)
    return 0.5 * dirichlet + 0.5 * uniform


def _kendall_tau(x: torch.Tensor, y: torch.Tensor) -> float:
    """Compute Kendall tau for small vectors without external deps. See src/training/README.md for details."""
    n = int(x.numel())
    if n < 2:
        return 0.0
    dx = x.unsqueeze(0) - x.unsqueeze(1)
    dy = y.unsqueeze(0) - y.unsqueeze(1)
    upper = torch.triu(torch.ones_like(dx, dtype=torch.bool), diagonal=1)
    tie = dy.abs() < KENDALL_TIE_THRESHOLD
    prod = dx * dy
    concordant = int(((prod > 0) & upper & ~tie).sum().item())
    discordant = int(((prod < 0) & upper & ~tie).sum().item())
    denom = max(1, concordant + discordant)
    return float((concordant - discordant) / denom)


def _model_state_on_cpu(model: torch.nn.Module) -> dict[str, torch.Tensor]:
    """Copy a model state dict to CPU tensors for best-epoch restoration."""
    return {name: value.detach().cpu().clone() for name, value in model.state_dict().items()}


def _ranking_loss_for_type(
    pred: torch.Tensor,
    target: torch.Tensor,
    valid_mask: torch.Tensor,
    pairs_per_type: int,
    top_quantile: float,
    margin: float,
    generator: torch.Generator,
) -> tuple[torch.Tensor, int]:
    """Compute top-boundary-focused pairwise ranking loss for one type. See src/training/README.md for details."""
    valid_idx = torch.where(valid_mask)[0]
    if valid_idx.numel() < 2:
        return pred.new_tensor(0.0), 0

    y = target[valid_idx]
    q_val = _safe_quantile(y, torch.tensor(top_quantile, dtype=torch.float32, device=y.device))
    top_idx = valid_idx[y >= q_val]
    strict_top_idx = valid_idx[y > q_val]
    if strict_top_idx.numel() > 0 and top_idx.numel() > max(4, valid_idx.numel() // 2):
        top_idx = strict_top_idx
    if top_idx.numel() == 0:
        top_idx = valid_idx

    # Sample pairs one at a time to preserve the interleaved RNG consumption
    # of the original implementation (bit-exact on CPU).  The loss is then
    # computed in a single batched call instead of per-pair, which avoids
    # autograd-graph blowup and lets the backward pass fuse efficiently.
    target_cpu = target.detach().cpu() if target.is_cuda else target
    top_idx_cpu = top_idx.cpu() if top_idx.is_cuda else top_idx
    valid_idx_cpu = valid_idx.cpu() if valid_idx.is_cuda else valid_idx
    a_list: list[int] = []
    b_list: list[int] = []
    for _ in range(pairs_per_type):
        ai = int(torch.randint(0, top_idx_cpu.numel(), (1,), generator=generator).item())
        bi = int(torch.randint(0, valid_idx_cpu.numel(), (1,), generator=generator).item())
        a = int(top_idx_cpu[ai].item())
        b = int(valid_idx_cpu[bi].item())
        if a == b:
            continue
        if bool(torch.isclose(target_cpu[a], target_cpu[b]).item()):
            continue
        a_list.append(a)
        b_list.append(b)

    if not a_list:
        return pred.new_tensor(0.0), 0

    a_idx = torch.tensor(a_list, dtype=torch.long, device=pred.device)
    b_idx = torch.tensor(b_list, dtype=torch.long, device=pred.device)
    sign = torch.sign(target[a_idx] - target[b_idx])
    return F.margin_ranking_loss(pred[a_idx], pred[b_idx], sign, margin=margin), len(a_list)


def _selection_score(avg_tau: float, pred_std: float, loss: float | None = None) -> float:
    """Score checkpoint quality while strongly penalizing collapsed predictions."""
    collapse_penalty = 1.0 if pred_std < 1e-3 else 0.0
    if loss is None:
        return float(avg_tau - collapse_penalty)
    return float(-float(loss) + 1e-3 * avg_tau - collapse_penalty)


def _f1_selection_score(query_f1: float, pred_std: float) -> float:
    """Score checkpoints by final query-F1 while rejecting collapsed predictions."""
    collapse_penalty = 1.0 if pred_std < 1e-3 else 0.0
    return float(query_f1 - collapse_penalty)


def _normalized_mix_dict(workload_mix: dict[str, float]) -> dict[str, float]:
    """Normalize workload weights by query type name."""
    names = ["range", "knn", "similarity", "clustering"]
    raw = {name: max(0.0, float(workload_mix.get(name, 0.0))) for name in names}
    total = sum(raw.values())
    if total <= 0.0:
        return {name: 1.0 / float(len(names)) for name in names}
    return {name: value / total for name, value in raw.items()}


def _uniform_type_deficit(
    per_type_f1: dict[str, float],
    uniform_per_type: dict[str, float],
    workload_mix: dict[str, float],
) -> float:
    """Weighted amount by which a checkpoint loses to fair uniform per type."""
    type_weights = _normalized_mix_dict(workload_mix)
    return float(
        sum(
            type_weights[name] * max(0.0, float(uniform_per_type.get(name, 0.0)) - float(per_type_f1.get(name, 0.0)))
            for name in type_weights
        )
    )


def _uniform_gap_selection_score(
    query_f1: float,
    per_type_f1: dict[str, float],
    uniform_f1: float,
    uniform_per_type: dict[str, float],
    workload_mix: dict[str, float],
    pred_std: float,
    aggregate_gap_weight: float = 0.5,
    type_penalty_weight: float = 1.0,
) -> float:
    """Score checkpoints by held-out F1 while penalizing losses to fair uniform."""
    collapse_penalty = 1.0 if pred_std < 1e-3 else 0.0
    aggregate_gap = float(query_f1) - float(uniform_f1)
    type_deficit = _uniform_type_deficit(per_type_f1, uniform_per_type, workload_mix)
    return float(
        float(query_f1)
        + float(aggregate_gap_weight) * aggregate_gap
        - float(type_penalty_weight) * type_deficit
        - collapse_penalty
    )


def _workload_mix_tensor(workload_mix: dict[str, float], device: torch.device) -> torch.Tensor:
    """Return normalized type weights in model-head order."""
    values = torch.tensor(
        [
            float(workload_mix.get("range", 0.0)),
            float(workload_mix.get("knn", 0.0)),
            float(workload_mix.get("similarity", 0.0)),
            float(workload_mix.get("clustering", 0.0)),
        ],
        dtype=torch.float32,
        device=device,
    )
    total = float(values.sum().item())
    if total <= 0.0:
        return torch.ones_like(values) / values.numel()
    return values / total


def _query_frequency_mix(workload: TypedQueryWorkload) -> dict[str, float]:
    """Infer type weights from a workload when no explicit training mix is provided."""
    counts = torch.bincount(workload.type_ids.detach().cpu(), minlength=NUM_QUERY_TYPES).float()
    return {
        "range": float(counts[0].item()),
        "knn": float(counts[1].item()),
        "similarity": float(counts[2].item()),
        "clustering": float(counts[3].item()),
    }


def _combine_epoch_type_weights(base_weights: torch.Tensor, epoch_mix: torch.Tensor) -> torch.Tensor:
    """Blend configured workload weights with stochastic epoch weights."""
    combined = base_weights * epoch_mix.to(base_weights.device)
    total = combined.sum()
    if float(total.item()) <= 0.0:
        return base_weights
    return combined / total


def _predict_workload_scores(
    model: TrajectoryQDSModel,
    scaler: FeatureScaler,
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    workload: TypedQueryWorkload,
    workload_mix: dict[str, float],
    model_config: ModelConfig,
    device: torch.device,
) -> torch.Tensor:
    """Predict workload-weighted simplification scores for exact query-F1 diagnostics."""
    point_dim = model.point_dim
    norm_points, norm_queries = scaler.transform(points[:, :point_dim].float(), workload.query_features)
    norm_points_dev = norm_points.to(device)
    norm_queries_dev = norm_queries.to(device)
    type_ids_dev = workload.type_ids.to(device)
    windows = build_trajectory_windows(
        points=norm_points,
        boundaries=boundaries,
        window_length=model_config.window_length,
        stride=model_config.window_stride,
    )
    windows = [
        TrajectoryBatch(
            points=window.points.to(device),
            padding_mask=window.padding_mask.to(device),
            trajectory_ids=window.trajectory_ids,
            global_indices=window.global_indices.to(device),
        )
        for window in windows
    ]
    windows = batch_windows(windows, max(1, int(getattr(model_config, "train_batch_size", 1))))
    all_pred = norm_points_dev.new_zeros((norm_points_dev.shape[0], NUM_QUERY_TYPES))
    pred_count = norm_points_dev.new_zeros((norm_points_dev.shape[0],))

    model.eval()
    with torch.no_grad():
        for window in windows:
            pred = model(
                points=window.points,
                queries=norm_queries_dev,
                query_type_ids=type_ids_dev,
                padding_mask=window.padding_mask,
            )
            for batch_idx in range(pred.shape[0]):
                global_idx = window.global_indices[batch_idx]
                valid = global_idx >= 0
                all_pred[global_idx[valid]] = all_pred[global_idx[valid]] + pred[batch_idx][valid]
                pred_count[global_idx[valid]] = pred_count[global_idx[valid]] + 1.0

    mix = _workload_mix_tensor(workload_mix, device=device)
    pred_count = pred_count.clamp(min=1.0)
    typed_pred = torch.sigmoid(all_pred / pred_count.unsqueeze(1))
    return (typed_pred * mix.unsqueeze(0)).sum(dim=1).detach().cpu()


def _validation_query_f1(
    model: TrajectoryQDSModel,
    scaler: FeatureScaler,
    trajectories: list[torch.Tensor],
    boundaries: list[tuple[int, int]],
    workload: TypedQueryWorkload,
    workload_mix: dict[str, float],
    model_config: ModelConfig,
    device: torch.device,
) -> tuple[float, dict[str, float]]:
    """Evaluate a model checkpoint with the same query-F1 semantics as final evaluation."""
    from src.evaluation.evaluate_methods import score_retained_mask

    points = torch.cat(trajectories, dim=0)
    scores = _predict_workload_scores(
        model=model,
        scaler=scaler,
        points=points,
        boundaries=boundaries,
        workload=workload,
        workload_mix=workload_mix,
        model_config=model_config,
        device=device,
    )
    mode = str(getattr(model_config, "simplification_mode", "score_coverage")).lower()
    if mode == "score_coverage":
        retained_mask = simplify_with_score_and_coverage(
            scores,
            boundaries,
            model_config.compression_ratio,
            coverage_lambda=float(getattr(model_config, "coverage_lambda", 0.5)),
            coverage_sigma_fraction=float(getattr(model_config, "coverage_sigma_fraction", 0.5)),
        )
    elif mode == "topk":
        retained_mask = simplify_with_scores(scores, boundaries, model_config.compression_ratio)
    else:
        retained_mask = simplify_with_temporal_score_hybrid(
            scores,
            boundaries,
            model_config.compression_ratio,
            temporal_fraction=float(getattr(model_config, "mlqds_temporal_fraction", 0.50)),
            diversity_bonus=float(getattr(model_config, "mlqds_diversity_bonus", 0.05)),
        )
    answer_agg, answer_pt, combined_agg, combined_pt = score_retained_mask(
        points=points,
        boundaries=boundaries,
        retained_mask=retained_mask,
        typed_queries=workload.typed_queries,
        workload_mix=workload_mix,
    )
    variant = str(getattr(model_config, "checkpoint_f1_variant", "answer")).lower()
    if variant == "combined":
        return combined_agg, combined_pt
    return answer_agg, answer_pt


def _validation_uniform_f1(
    trajectories: list[torch.Tensor],
    boundaries: list[tuple[int, int]],
    workload: TypedQueryWorkload,
    workload_mix: dict[str, float],
    model_config: ModelConfig,
) -> tuple[float, dict[str, float]]:
    """Evaluate fair uniform on the held-out validation workload once per run."""
    from src.evaluation.baselines import NewUniformTemporalMethod
    from src.evaluation.evaluate_methods import score_retained_mask

    points = torch.cat(trajectories, dim=0)
    retained_mask = NewUniformTemporalMethod().simplify(
        points=points,
        boundaries=boundaries,
        compression_ratio=model_config.compression_ratio,
    )
    answer_agg, answer_pt, combined_agg, combined_pt = score_retained_mask(
        points=points,
        boundaries=boundaries,
        retained_mask=retained_mask,
        typed_queries=workload.typed_queries,
        workload_mix=workload_mix,
    )
    variant = str(getattr(model_config, "checkpoint_f1_variant", "answer")).lower()
    if variant == "combined":
        return combined_agg, combined_pt
    return answer_agg, answer_pt


def train_model(
    train_trajectories: list[torch.Tensor],
    train_boundaries: list[tuple[int, int]],
    workload: TypedQueryWorkload,
    model_config: ModelConfig,
    seed: int,
    train_mix: dict[str, float] | None = None,
    validation_trajectories: list[torch.Tensor] | None = None,
    validation_boundaries: list[tuple[int, int]] | None = None,
    validation_workload: TypedQueryWorkload | None = None,
    validation_mix: dict[str, float] | None = None,
) -> TrainingOutputs:
    """Train typed-head model with trajectory-window ranking losses. See src/training/README.md for details."""
    torch.manual_seed(int(seed))
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(int(seed))

    all_points = torch.cat(train_trajectories, dim=0)
    point_dim = 8 if model_config.model_type == "turn_aware" else 7
    points = all_points[:, :point_dim].float()

    labels, labelled_mask = compute_typed_importance_labels(
        points=all_points,
        boundaries=train_boundaries,
        typed_queries=workload.typed_queries,
        seed=seed,
    )
    residual_label_mode = str(getattr(model_config, "residual_label_mode", "none")).lower()
    if residual_label_mode not in {"none", "temporal"}:
        raise ValueError("residual_label_mode must be 'none' or 'temporal'.")
    if residual_label_mode == "temporal":
        labels, labelled_mask = _apply_temporal_residual_labels(
            labels=labels,
            labelled_mask=labelled_mask,
            boundaries=train_boundaries,
            compression_ratio=model_config.compression_ratio,
            temporal_fraction=float(getattr(model_config, "mlqds_temporal_fraction", 0.50)),
        )
    training_targets = _scaled_training_targets(labels, labelled_mask)
    active_type_ids = [t for t in range(NUM_QUERY_TYPES) if bool(labelled_mask[:, t].any().item())]
    if not active_type_ids:
        active_type_ids = list(range(NUM_QUERY_TYPES))

    scaler = FeatureScaler.fit(points, workload.query_features)
    norm_points, norm_queries = scaler.transform(points, workload.query_features)

    model_cls = TurnAwareQDSModel if model_config.model_type == "turn_aware" else TrajectoryQDSModel
    model = model_cls(
        point_dim=point_dim,
        query_dim=norm_queries.shape[1],
        embed_dim=model_config.embed_dim,
        num_heads=model_config.num_heads,
        num_layers=model_config.num_layers,
        type_embed_dim=model_config.type_embed_dim,
        query_chunk_size=model_config.query_chunk_size,
        dropout=model_config.dropout,
        use_cls_token=bool(getattr(model_config, "use_cls_token", True)),
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    norm_points_dev = norm_points.to(device)
    norm_queries_dev = norm_queries.to(device)
    type_ids_dev = workload.type_ids.to(device)
    training_targets_dev = training_targets.to(device)
    labelled_mask_dev = labelled_mask.to(device)
    base_type_weights = _workload_mix_tensor(train_mix or _query_frequency_mix(workload), device=device)

    opt = torch.optim.Adam(model.parameters(), lr=model_config.lr)
    windows_cpu = build_trajectory_windows(
        points=norm_points,
        boundaries=train_boundaries,
        window_length=model_config.window_length,
        stride=model_config.window_stride,
    )
    single_windows = [
        TrajectoryBatch(
            points=w.points.to(device),
            padding_mask=w.padding_mask.to(device),
            trajectory_ids=w.trajectory_ids,
            global_indices=w.global_indices.to(device),
        )
        for w in windows_cpu
    ]
    train_batch_size = max(1, int(getattr(model_config, "train_batch_size", 1)))
    windows = batch_windows(single_windows, train_batch_size)
    # Diagnostic pass operates on single windows so we can cheaply subsample a
    # fraction of them (batched diagnostics would waste the remaining lanes).
    diag_windows = single_windows
    diag_every = max(1, int(getattr(model_config, "diagnostic_every", 1)))
    diag_fraction = float(getattr(model_config, "diagnostic_window_fraction", 1.0))
    diag_fraction = min(1.0, max(0.05, diag_fraction))
    run_tag = "main"
    selection_metric = str(getattr(model_config, "checkpoint_selection_metric", "loss")).lower()
    if selection_metric not in {"loss", "f1", "uniform_gap"}:
        raise ValueError("checkpoint_selection_metric must be 'loss', 'f1', or 'uniform_gap'.")
    f1_diag_every = int(getattr(model_config, "f1_diagnostic_every", 0) or 0)
    has_validation_f1 = (
        validation_trajectories is not None
        and validation_boundaries is not None
        and validation_workload is not None
        and validation_mix is not None
    )
    if selection_metric in {"f1", "uniform_gap"} and not has_validation_f1:
        print(
            f"  [{run_tag}] WARNING: checkpoint_selection_metric={selection_metric} requested without validation workload; "
            "falling back to loss selection.",
            flush=True,
        )
        selection_metric = "loss"
    if selection_metric in {"f1", "uniform_gap"} and f1_diag_every <= 0:
        f1_diag_every = diag_every
    validation_uniform_result: tuple[float, dict[str, float]] | None = None
    if selection_metric == "uniform_gap" and has_validation_f1:
        validation_uniform_result = _validation_uniform_f1(
            trajectories=validation_trajectories or [],
            boundaries=validation_boundaries or [],
            workload=validation_workload,
            workload_mix=validation_mix or {},
            model_config=model_config,
        )
        uniform_f1, uniform_per_type = validation_uniform_result
        print(
            f"  [{run_tag}] validation uniform_f1={uniform_f1:.6f}  "
            f"range={uniform_per_type.get('range', 0.0):.6f}  "
            f"knn={uniform_per_type.get('knn', 0.0):.6f}  "
            f"similarity={uniform_per_type.get('similarity', 0.0):.6f}  "
            f"clustering={uniform_per_type.get('clustering', 0.0):.6f}",
            flush=True,
        )

    g = torch.Generator().manual_seed(int(seed) + 99)
    # Separate fixed-seed generator for diagnostics so the tau subsample
    # stays consistent across epochs and doesn't oscillate with training state.
    eval_g = torch.Generator().manual_seed(int(seed) + 777)
    history: list[dict[str, float]] = []

    effective_epochs = max(8, int(model_config.epochs))
    patience = int(getattr(model_config, "early_stopping_patience", 0) or 0)
    smoothing_window = max(1, int(getattr(model_config, "checkpoint_smoothing_window", 1) or 1))
    selection_history: list[float] = []
    best_selection = float("-inf")
    best_loss = float("inf")
    best_f1 = 0.0
    best_epoch = 0
    best_state_dict: dict[str, torch.Tensor] | None = None
    epochs_no_improve = 0
    epoch_w = len(str(effective_epochs))
    epochs_trained = 0
    for epoch in range(effective_epochs):
        epoch_t0 = time.perf_counter()
        model.train()
        epoch_mix = _sample_epoch_mix(model_config.dirichlet_alpha, g).to(device)
        epoch_type_weights = _combine_epoch_type_weights(base_type_weights, epoch_mix)
        epoch_loss = torch.tensor(0.0, device=device)
        positive_windows = torch.zeros((NUM_QUERY_TYPES,), dtype=torch.long)
        skipped_zero_windows = torch.zeros((NUM_QUERY_TYPES,), dtype=torch.long)
        ranking_pair_counts = torch.zeros((NUM_QUERY_TYPES,), dtype=torch.long)

        for w in windows:
            pred_batch = model(
                points=w.points,
                queries=norm_queries_dev,
                query_type_ids=type_ids_dev,
                padding_mask=w.padding_mask,
            )
            # pred_batch: (B, L, T).  Accumulate per-window loss terms across
            # the batch and backprop once per batch — this is what makes the
            # GPU actually saturated compared to the old batch=1 loop.
            loss_terms: list[torch.Tensor] = []
            B = pred_batch.shape[0]
            for b in range(B):
                idx = w.global_indices[b]
                valid_window = idx >= 0
                global_idx = idx[valid_window]
                pred_valid = pred_batch[b][valid_window]

                for t in range(NUM_QUERY_TYPES):
                    type_weight = epoch_type_weights[t]
                    if float(type_weight.item()) <= 0.0:
                        continue
                    t_labels = training_targets_dev[global_idx, t]
                    t_mask = labelled_mask_dev[global_idx, t]
                    if not bool((t_mask & (t_labels > 0)).any().item()):
                        skipped_zero_windows[t] += 1
                        continue
                    t_pred = pred_valid[:, t]
                    positive_windows[t] += 1
                    rank_loss, pair_count = _ranking_loss_for_type(
                        pred=t_pred,
                        target=t_labels,
                        valid_mask=t_mask,
                        pairs_per_type=model_config.ranking_pairs_per_type,
                        top_quantile=model_config.ranking_top_quantile,
                        margin=model_config.rank_margin,
                        generator=g,
                    )
                    ranking_pair_counts[t] += int(pair_count)
                    pointwise_term = _balanced_pointwise_loss(t_pred, t_labels, t_mask, generator=g)
                    loss_terms.append(type_weight * (rank_loss + model_config.pointwise_loss_weight * pointwise_term))

            if loss_terms:
                loss = (
                    torch.stack(loss_terms).sum() / float(B)
                    + model_config.l2_score_weight * (pred_batch ** 2).mean()
                )
                opt.zero_grad()
                loss.backward()
                clip_norm = float(getattr(model_config, "gradient_clip_norm", 0.0) or 0.0)
                if clip_norm > 0.0:
                    torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=clip_norm)
                opt.step()
                epoch_loss = epoch_loss + loss.detach()

        # Diagnostic pass only on selected epochs (every `diag_every` epochs and
        # the final epoch).  Subsample windows by `diag_fraction` to further cut
        # cost: pred_std and tau are statistical aggregates and noise from a
        # ~20% sample is tiny compared to the training noise we're measuring.
        is_last_epoch = (epoch + 1) == effective_epochs
        is_diag_epoch = ((epoch + 1) % diag_every == 0) or is_last_epoch or epoch == 0
        if is_diag_epoch:
            if diag_fraction < 1.0 and len(diag_windows) > 8:
                k = max(8, int(len(diag_windows) * diag_fraction))
                perm = torch.randperm(len(diag_windows), generator=eval_g)[:k].tolist()
                sample_windows = [diag_windows[i] for i in perm]
            else:
                sample_windows = diag_windows

            model.eval()
            with torch.no_grad():
                all_pred = norm_points_dev.new_zeros((norm_points_dev.shape[0], NUM_QUERY_TYPES))
                pred_count = norm_points_dev.new_zeros((norm_points_dev.shape[0],))
                for w in sample_windows:
                    wp = model(
                        points=w.points,
                        queries=norm_queries_dev,
                        query_type_ids=type_ids_dev,
                        padding_mask=w.padding_mask,
                    )[0]
                    widx = w.global_indices[0]
                    valid = widx >= 0
                    all_pred[widx[valid]] = all_pred[widx[valid]] + wp[valid]
                    pred_count[widx[valid]] = pred_count[widx[valid]] + 1.0
                covered_mask = pred_count > 0
                pred_count = pred_count.clamp(min=1.0)
                full_pred = all_pred / pred_count.unsqueeze(1)

            stats: dict[str, float] = {
                "epoch": float(epoch),
                "loss": float(epoch_loss.item() / max(1, len(windows))),
                "pred_std": (
                    float(full_pred[covered_mask][:, active_type_ids].std().item())
                    if bool(covered_mask.any().item())
                    else 0.0
                ),
            }
            for type_idx in range(NUM_QUERY_TYPES):
                stats[f"positive_windows_t{type_idx}"] = float(positive_windows[type_idx].item())
                stats[f"skipped_zero_windows_t{type_idx}"] = float(skipped_zero_windows[type_idx].item())
                stats[f"ranking_pairs_t{type_idx}"] = float(ranking_pair_counts[type_idx].item())
            for t in range(NUM_QUERY_TYPES):
                pt = full_pred[:, t]
                stats[f"pred_p50_t{t}"] = float(_safe_quantile(pt, 0.50).item())
                stats[f"pred_p90_t{t}"] = float(_safe_quantile(pt, 0.90).item())
                stats[f"pred_p99_t{t}"] = float(_safe_quantile(pt, 0.99).item())
                labelled_type = labelled_mask_dev[:, t]
                positive_type = labelled_type & (training_targets_dev[:, t] > 0)
                labelled_count = max(1, int(labelled_type.sum().item()))
                stats[f"positive_fraction_t{t}"] = float(positive_type.sum().item() / labelled_count)
                if bool(positive_type.any().item()):
                    stats[f"label_p95_t{t}"] = float(_safe_quantile(training_targets_dev[positive_type, t], 0.95).item())
                else:
                    stats[f"label_p95_t{t}"] = 0.0
                eval_mask = labelled_mask_dev[:, t] & covered_mask
                if bool(eval_mask.any().item()):
                    # Reset eval_g to the same state each epoch so the diagnostic
                    # subsample is identical across epochs, giving stable tau trends.
                    eval_g.manual_seed(int(seed) + 777)
                    p_sample, y_sample = _discriminative_sample(
                        pt[eval_mask].detach().cpu(),
                        training_targets_dev[eval_mask, t].detach().cpu(),
                        n_each=100,
                        generator=eval_g,
                    )
                    stats[f"kendall_tau_t{t}"] = _kendall_tau(p_sample, y_sample)
                else:
                    stats[f"kendall_tau_t{t}"] = 0.0

            if stats["pred_std"] < 1e-3:
                stats["collapse_warning"] = 1.0

            should_run_f1 = has_validation_f1 and (
                selection_metric == "f1"
                or (f1_diag_every > 0 and ((epoch + 1) % f1_diag_every == 0 or is_last_epoch))
            )
            if should_run_f1:
                query_f1, per_type_f1 = _validation_query_f1(
                    model=model,
                    scaler=scaler,
                    trajectories=validation_trajectories or [],
                    boundaries=validation_boundaries or [],
                    workload=validation_workload,
                    workload_mix=validation_mix or {},
                    model_config=model_config,
                    device=device,
                )
                stats["val_query_f1"] = float(query_f1)
                for type_name, value in per_type_f1.items():
                    stats[f"val_query_f1_{type_name}"] = float(value)
                if validation_uniform_result is not None:
                    uniform_f1, uniform_per_type = validation_uniform_result
                    stats["val_uniform_f1"] = float(uniform_f1)
                    stats["val_query_uniform_gap"] = float(query_f1 - uniform_f1)
                    stats["val_query_type_deficit"] = _uniform_type_deficit(
                        per_type_f1,
                        uniform_per_type,
                        validation_mix or {},
                    )
                    for type_name, value in uniform_per_type.items():
                        stats[f"val_uniform_f1_{type_name}"] = float(value)
                        stats[f"val_query_f1_gap_{type_name}"] = float(per_type_f1.get(type_name, 0.0) - value)
        else:
            # Skip diagnostics this epoch; log only loss.  Patience counters
            # are only updated on diagnostic epochs below.
            stats = {
                "epoch": float(epoch),
                "loss": float(epoch_loss.item() / max(1, len(windows))),
            }
            for type_idx in range(NUM_QUERY_TYPES):
                stats[f"positive_windows_t{type_idx}"] = float(positive_windows[type_idx].item())
                stats[f"skipped_zero_windows_t{type_idx}"] = float(skipped_zero_windows[type_idx].item())
                stats[f"ranking_pairs_t{type_idx}"] = float(ranking_pair_counts[type_idx].item())

        history.append(stats)

        epoch_dt = time.perf_counter() - epoch_t0
        epochs_trained = epoch + 1

        if is_diag_epoch:
            tau_vals = [stats[f"kendall_tau_t{t}"] for t in active_type_ids]
            avg_tau = sum(tau_vals) / max(1, len(tau_vals))
            collapse = "  COLLAPSE" if stats.get("collapse_warning") else ""
            if selection_metric == "uniform_gap" and "val_query_f1" in stats and validation_uniform_result is not None:
                uniform_f1, uniform_per_type = validation_uniform_result
                per_type_f1 = {
                    name: stats.get(f"val_query_f1_{name}", 0.0)
                    for name in ["range", "knn", "similarity", "clustering"]
                }
                selection = _uniform_gap_selection_score(
                    query_f1=stats["val_query_f1"],
                    per_type_f1=per_type_f1,
                    uniform_f1=uniform_f1,
                    uniform_per_type=uniform_per_type,
                    workload_mix=validation_mix or {},
                    pred_std=stats["pred_std"],
                    aggregate_gap_weight=float(getattr(model_config, "checkpoint_uniform_gap_weight", 0.5)),
                    type_penalty_weight=float(getattr(model_config, "checkpoint_type_penalty_weight", 1.0)),
                )
            elif selection_metric == "f1" and "val_query_f1" in stats:
                selection = _f1_selection_score(stats["val_query_f1"], stats["pred_std"])
            else:
                selection = _selection_score(avg_tau, stats["pred_std"], stats["loss"])
            stats["selection_score"] = selection
            selection_history.append(float(selection))
            window = selection_history[-smoothing_window:]
            smoothed_selection = float(sum(window) / len(window))
            stats["selection_score_smoothed"] = smoothed_selection
            # Use the smoothed score for "best" decisions: averages out
            # epoch-to-epoch validation F1 noise so we don't lock onto a lucky
            # spike. Single-epoch loss still tiebreaks on near-equal smoothed.
            is_new_best_model = smoothed_selection > best_selection + 1e-4 or (
                abs(smoothed_selection - best_selection) <= 1e-4 and stats["loss"] < best_loss - 1e-8
            )
            markers = []
            if epoch > 0 and is_new_best_model:
                markers.append("*** NEW BEST MODEL ***")
            best_marker = ("  " + "  ".join(markers)) if markers else ""
            smoothed_label = (
                f"  smoothed_w{smoothing_window}={smoothed_selection:+.3f}"
                if smoothing_window > 1 else ""
            )
            print(
                f"  [{run_tag}] epoch {epoch + 1:0{epoch_w}d}/{effective_epochs}  "
                f"loss={stats['loss']:.8f}  avg_tau={avg_tau:+.3f}  "
                f"pred_std={stats['pred_std']:.6g}  select={selection:+.3f}{smoothed_label}  "
                f"({epoch_dt:.2f}s){collapse}{best_marker}",
                flush=True,
            )
            if "val_query_f1" in stats:
                print(
                    f"    [{run_tag}] val_query_f1={stats['val_query_f1']:.6f}  "
                    f"range={stats.get('val_query_f1_range', 0.0):.6f}  "
                    f"knn={stats.get('val_query_f1_knn', 0.0):.6f}  "
                    f"similarity={stats.get('val_query_f1_similarity', 0.0):.6f}  "
                    f"clustering={stats.get('val_query_f1_clustering', 0.0):.6f}",
                    flush=True,
                )
            if "val_uniform_f1" in stats:
                print(
                    f"    [{run_tag}] val_vs_uniform aggregate={stats['val_query_uniform_gap']:+.6f}  "
                    f"type_deficit={stats['val_query_type_deficit']:.6f}  "
                    f"range={stats.get('val_query_f1_gap_range', 0.0):+.6f}  "
                    f"knn={stats.get('val_query_f1_gap_knn', 0.0):+.6f}  "
                    f"similarity={stats.get('val_query_f1_gap_similarity', 0.0):+.6f}  "
                    f"clustering={stats.get('val_query_f1_gap_clustering', 0.0):+.6f}",
                    flush=True,
                )
            diag_parts = []
            for type_idx in active_type_ids:
                type_name = ID_TO_QUERY_NAME.get(type_idx, f"t{type_idx}")
                diag_parts.append(
                    f"{type_name}:pos={stats[f'positive_fraction_t{type_idx}']:.4f},"
                    f"p95={stats[f'label_p95_t{type_idx}']:.3f},"
                    f"pairs={int(stats[f'ranking_pairs_t{type_idx}'])},"
                    f"skip={int(stats[f'skipped_zero_windows_t{type_idx}'])}"
                )
            if diag_parts:
                print(f"    [{run_tag}] label_diag  " + "  ".join(diag_parts), flush=True)

            if is_new_best_model:
                best_selection = smoothed_selection
                best_loss = stats["loss"]
                best_f1 = float(stats.get("val_query_f1", best_f1))
                best_epoch = epoch + 1
                best_state_dict = _model_state_on_cpu(model)

            if patience > 0:
                if is_new_best_model:
                    epochs_no_improve = 0
                else:
                    epochs_no_improve += 1
                    if epochs_no_improve >= patience:
                        print(
                            f"  [{run_tag}] early stopping at epoch {epoch + 1:0{epoch_w}d}: "
                            f"selection score did not improve over {patience} diag epochs "
                            f"(best_selection={best_selection:+.3f}, best_loss={best_loss:.8f})",
                            flush=True,
                        )
                        break
        else:
            # Non-diagnostic epoch: log loss only, no tau / early-stopping update.
            print(
                f"  [{run_tag}] epoch {epoch + 1:0{epoch_w}d}/{effective_epochs}  "
                f"loss={stats['loss']:.8f}  (no-diag)  ({epoch_dt:.2f}s)",
                flush=True,
            )

    model = model.to("cpu")
    if best_state_dict is not None:
        model.load_state_dict(best_state_dict)
        print(
            f"  [{run_tag}] restored best diagnostic epoch {best_epoch}/{epochs_trained} "
            f"(selection={best_selection:+.3f}, loss={best_loss:.8f}, val_f1={best_f1:.6f})",
            flush=True,
        )
    return TrainingOutputs(
        model=model,
        scaler=scaler,
        labels=labels,
        labelled_mask=labelled_mask,
        history=history,
        epochs_trained=epochs_trained,
        best_epoch=best_epoch,
        best_loss=best_loss,
        best_f1=best_f1,
    )
