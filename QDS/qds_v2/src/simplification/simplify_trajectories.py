"""Per-trajectory top-k simplification utilities. See src/simplification/README.md for details."""

from __future__ import annotations

import math

import torch


def deterministic_topk_with_jitter(
    scores: torch.Tensor,
    k: int,
    trajectory_id: int,
) -> torch.Tensor:
    """Select top-k indices with deterministic pseudo-random tie jitter. See src/simplification/README.md for details."""
    n = scores.numel()
    if k >= n:
        return torch.arange(n, dtype=torch.long, device=scores.device)

    pos = torch.arange(n, device=scores.device, dtype=torch.float32)
    # Deterministic hash-like noise in [-0.5, 0.5].
    noise = torch.frac(torch.sin(pos * 12.9898 + float(trajectory_id) * 78.233) * 43758.5453) - 0.5
    jittered = scores + 1e-6 * noise
    top = torch.topk(jittered, k=k, largest=True).indices
    return torch.sort(top).values


def evenly_spaced_indices(n: int, k: int, device: torch.device) -> torch.Tensor:
    """Return deterministic evenly spaced local indices, including endpoints when possible."""
    n = int(n)
    k = max(0, min(int(k), n))
    if k <= 0 or n <= 0:
        return torch.empty((0,), dtype=torch.long, device=device)
    if k >= n:
        return torch.arange(n, dtype=torch.long, device=device)
    local_idx = torch.linspace(0, n - 1, steps=k, device=device).round().long().unique()
    if local_idx.numel() < k:
        filler = torch.arange(n, dtype=torch.long, device=device)
        missing = filler[~torch.isin(filler, local_idx)][: k - local_idx.numel()]
        local_idx = torch.cat([local_idx, missing])
    return torch.sort(local_idx).values


def simplify_with_scores(
    scores: torch.Tensor,
    boundaries: list[tuple[int, int]],
    compression_ratio: float,
) -> torch.Tensor:
    """Build retained mask by per-trajectory score top-k. See src/simplification/README.md for details."""
    retained = torch.zeros(scores.shape[0], dtype=torch.bool, device=scores.device)
    for tid, (start, end) in enumerate(boundaries):
        local = scores[start:end]
        n = local.numel()
        k = max(2, int(math.ceil(compression_ratio * n)))
        idx = deterministic_topk_with_jitter(local, k=k, trajectory_id=tid)
        retained[start:end][idx] = True
        retained[start] = True
        retained[end - 1] = True
    return retained


def simplify_with_temporal_score_hybrid(
    scores: torch.Tensor,
    boundaries: list[tuple[int, int]],
    compression_ratio: float,
    temporal_fraction: float = 0.75,
    diversity_bonus: float = 0.05,
) -> torch.Tensor:
    """Retain a temporal coverage base, then fill remaining slots by learned score.

    Pure top-k scoring tends to over-select neighbouring points with similar
    logits.  This hybrid keeps most of the strong evenly-spaced temporal
    baseline and lets MLQDS spend the remaining budget on query-aware points.
    """
    retained = torch.zeros(scores.shape[0], dtype=torch.bool, device=scores.device)
    base_fraction = min(1.0, max(0.0, float(temporal_fraction)))
    bonus = max(0.0, float(diversity_bonus))

    for tid, (start, end) in enumerate(boundaries):
        local = scores[start:end]
        n = local.numel()
        if n <= 0:
            continue
        k_total = max(2, int(math.ceil(float(compression_ratio) * n)))
        k_total = min(k_total, n)
        k_base = min(k_total, max(2, int(math.ceil(k_total * base_fraction))))
        base_idx = evenly_spaced_indices(n, k_base, scores.device)
        retained[start + base_idx] = True

        remaining = k_total - int(base_idx.numel())
        if remaining <= 0:
            continue

        candidate_scores = local.clone()
        candidate_scores[base_idx] = -float("inf")
        if bonus > 0.0 and base_idx.numel() > 0 and n > 1:
            pos = torch.arange(n, dtype=torch.float32, device=scores.device)
            dist_to_base = torch.abs(pos.unsqueeze(1) - base_idx.float().unsqueeze(0)).min(dim=1).values
            candidate_scores = candidate_scores + bonus * (dist_to_base / float(max(1, n - 1)))
            candidate_scores[base_idx] = -float("inf")

        fill_idx = deterministic_topk_with_jitter(candidate_scores, k=remaining, trajectory_id=tid)
        retained[start + fill_idx] = True

    return retained


def _greedy_score_with_coverage(
    local_scores: torch.Tensor,
    k: int,
    trajectory_id: int,
    coverage_lambda: float,
    coverage_sigma_fraction: float,
) -> torch.Tensor:
    """Iteratively pick k indices maximising score minus a Gaussian density penalty.

    The kernel is a sum of Gaussians centred on already-kept indices,
    scaled by ``coverage_lambda``. Sigma is set to a fraction of the expected
    spacing ``n / k`` so the penalty discourages selecting near-neighbours of
    already-kept points without forbidding them outright. This trades off the
    learned score against a positional spread guarantee — closing the coverage
    gap that pure top-k inflicts at tight compression.

    Endpoints (positions 0 and n-1) are seeded first so the simplification
    matches the boundary behaviour of uniform / DP and never strands the head
    or tail of a trajectory.
    """
    n = int(local_scores.numel())
    device = local_scores.device
    if n <= 0 or k <= 0:
        return torch.empty((0,), dtype=torch.long, device=device)
    if k >= n:
        return torch.arange(n, dtype=torch.long, device=device)

    pos = torch.arange(n, device=device, dtype=torch.float32)
    noise = torch.frac(torch.sin(pos * 12.9898 + float(trajectory_id) * 78.233) * 43758.5453) - 0.5
    base_score = local_scores + 1e-6 * noise

    expected_spacing = max(1.0, float(n) / float(max(2, k)))
    sigma = max(1.0, expected_spacing * float(coverage_sigma_fraction))
    inv_two_sigma_sq = 1.0 / (2.0 * sigma * sigma)
    lam = max(0.0, float(coverage_lambda))

    selected = torch.zeros(n, dtype=torch.bool, device=device)
    density = torch.zeros(n, dtype=torch.float32, device=device)

    def _add(idx: int) -> None:
        selected[idx] = True
        if lam > 0.0:
            delta = pos - float(idx)
            density.add_(torch.exp(-(delta * delta) * inv_two_sigma_sq))

    _add(0)
    kept = 1
    if k >= 2 and n >= 2:
        _add(n - 1)
        kept = 2

    while kept < k:
        adjusted = base_score - lam * density
        adjusted = adjusted.masked_fill(selected, float("-inf"))
        next_idx = int(torch.argmax(adjusted).item())
        # If every remaining slot has score -inf (e.g. lam massively over-penalised),
        # fall back to the first unselected index so the budget is still honoured.
        if selected[next_idx]:
            unsel = torch.where(~selected)[0]
            if unsel.numel() == 0:
                break
            next_idx = int(unsel[0].item())
        _add(next_idx)
        kept += 1

    return torch.where(selected)[0]


def simplify_with_score_and_coverage(
    scores: torch.Tensor,
    boundaries: list[tuple[int, int]],
    compression_ratio: float,
    coverage_lambda: float = 0.5,
    coverage_sigma_fraction: float = 0.5,
) -> torch.Tensor:
    """Per-trajectory coverage-aware greedy simplification.

    Differs from the temporal-hybrid variant in that there is no fixed temporal
    base: every retained point is chosen by the learned score, but each pick
    incurs a Gaussian density penalty centred on already-kept positions. With
    ``coverage_lambda=0`` this reduces to pure top-k; as lambda grows the
    selection approaches uniform spacing.
    """
    retained = torch.zeros(scores.shape[0], dtype=torch.bool, device=scores.device)
    for tid, (start, end) in enumerate(boundaries):
        local = scores[start:end]
        n = local.numel()
        if n <= 0:
            continue
        k = max(2, int(math.ceil(float(compression_ratio) * n)))
        k = min(k, n)
        idx = _greedy_score_with_coverage(
            local_scores=local,
            k=k,
            trajectory_id=tid,
            coverage_lambda=coverage_lambda,
            coverage_sigma_fraction=coverage_sigma_fraction,
        )
        retained[start:end][idx] = True
    return retained
