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


def _haversine_km_pairs(lat1: torch.Tensor, lon1: torch.Tensor, lat2: torch.Tensor, lon2: torch.Tensor) -> torch.Tensor:
    """Vectorised haversine distance in km between paired (lat,lon) points."""
    radius_km = 6371.0
    lat1_r = torch.deg2rad(lat1)
    lat2_r = torch.deg2rad(lat2)
    dlat = lat2_r - lat1_r
    dlon = torch.deg2rad(lon2) - torch.deg2rad(lon1)
    a = torch.sin(dlat / 2.0) ** 2 + torch.cos(lat1_r) * torch.cos(lat2_r) * torch.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * torch.atan2(torch.sqrt(a), torch.sqrt(torch.clamp(1.0 - a, min=1e-9)))


def _greedy_score_with_coverage(
    local_scores: torch.Tensor,
    k: int,
    trajectory_id: int,
    coverage_lambda: float,
    coverage_sigma_fraction: float,
    local_lats: torch.Tensor | None = None,
    local_lons: torch.Tensor | None = None,
    length_preservation_weight: float = 0.0,
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

    When ``length_preservation_weight > 0`` and lat/lon are provided, the
    selection score also gains a "shape" term: for each candidate i with
    bracketing already-kept neighbours j (left) and k (right), the term equals
    haversine(j,i)+haversine(i,k)-haversine(j,k) — the polyline-detour added by
    keeping i. Points off the j-k chord get a positive bonus, points on the
    chord get ~0. This pushes MLQDS to retain shape-defining points (similar
    to Douglas-Peucker) without giving up the learned score signal.
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
    mu = max(0.0, float(length_preservation_weight))
    use_length = mu > 0.0 and local_lats is not None and local_lons is not None and local_lats.numel() == n
    if use_length:
        local_lats = local_lats.to(device).float()
        local_lons = local_lons.to(device).float()

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
        if use_length:
            length_term = _length_signal_per_candidate(selected, local_lats, local_lons)
            # Normalise by trajectory's mean inter-point step so mu has a similar
            # scale across short and long trajectories. mu=1 means a +mean-step
            # length detour is worth as much as a +1 score unit.
            mean_step = float(_haversine_km_pairs(local_lats[:-1], local_lons[:-1], local_lats[1:], local_lons[1:]).mean().clamp(min=1e-3).item())
            adjusted = adjusted + (mu / mean_step) * length_term
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


def _length_signal_per_candidate(
    selected: torch.Tensor,
    lats: torch.Tensor,
    lons: torch.Tensor,
) -> torch.Tensor:
    """Per-candidate polyline-detour length in km, vectorised.

    For each unselected index i bracketed by selected j<i<k, returns
    haversine(j,i) + haversine(i,k) - haversine(j,k).
    Indices without left or right bracket get 0 (head/tail not yet selected).
    Selected indices get -inf so they're never re-picked.
    """
    n = int(selected.numel())
    device = selected.device
    out = torch.zeros(n, dtype=torch.float32, device=device)
    sel_pos = torch.where(selected)[0]
    if sel_pos.numel() < 2:
        return out
    cand_pos = torch.arange(n, device=device)
    # right_idx[i] = first selected index strictly > i (bisect_right for sorted sel_pos)
    right_idx = torch.searchsorted(sel_pos, cand_pos, right=True)
    left_idx = right_idx - 1
    valid = (~selected) & (left_idx >= 0) & (right_idx < sel_pos.numel())
    if not bool(valid.any().item()):
        return out
    vp = cand_pos[valid]
    j = sel_pos[left_idx[valid]]
    kk = sel_pos[right_idx[valid]]
    dji = _haversine_km_pairs(lats[j], lons[j], lats[vp], lons[vp])
    dik = _haversine_km_pairs(lats[vp], lons[vp], lats[kk], lons[kk])
    djk = _haversine_km_pairs(lats[j], lons[j], lats[kk], lons[kk])
    out[vp] = (dji + dik - djk).clamp(min=0.0)
    return out


def simplify_with_score_and_coverage(
    scores: torch.Tensor,
    boundaries: list[tuple[int, int]],
    compression_ratio: float,
    coverage_lambda: float = 0.5,
    coverage_sigma_fraction: float = 0.5,
    points: torch.Tensor | None = None,
    length_preservation_weight: float = 0.0,
) -> torch.Tensor:
    """Per-trajectory coverage-aware greedy simplification.

    Differs from the temporal-hybrid variant in that there is no fixed temporal
    base: every retained point is chosen by the learned score, but each pick
    incurs a Gaussian density penalty centred on already-kept positions. With
    ``coverage_lambda=0`` this reduces to pure top-k; as lambda grows the
    selection approaches uniform spacing.

    When ``length_preservation_weight > 0`` and ``points`` is given (with lat in
    column 1, lon in column 2), the greedy also rewards shape-defining points
    via a polyline-detour bonus (Douglas-Peucker-style). This is opt-in: with
    weight=0 the selection is bit-identical to the score+coverage path.
    """
    retained = torch.zeros(scores.shape[0], dtype=torch.bool, device=scores.device)
    for tid, (start, end) in enumerate(boundaries):
        local = scores[start:end]
        n = local.numel()
        if n <= 0:
            continue
        k = max(2, int(math.ceil(float(compression_ratio) * n)))
        k = min(k, n)
        local_lats = local_lons = None
        if points is not None and length_preservation_weight > 0.0 and points.shape[1] >= 3:
            local_lats = points[start:end, 1]
            local_lons = points[start:end, 2]
        idx = _greedy_score_with_coverage(
            local_scores=local,
            k=k,
            trajectory_id=tid,
            coverage_lambda=coverage_lambda,
            coverage_sigma_fraction=coverage_sigma_fraction,
            local_lats=local_lats,
            local_lons=local_lons,
            length_preservation_weight=length_preservation_weight,
        )
        retained[start:end][idx] = True
    return retained
