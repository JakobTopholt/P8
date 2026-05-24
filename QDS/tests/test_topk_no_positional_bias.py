"""Tests tie-handling top-k selection does not create positional bias. See src/simplification/README.md for details."""

from __future__ import annotations

import math

import torch

from src.simplification.simplify_trajectories import simplify_with_scores


def _ks_pvalue_uniform01(samples: torch.Tensor) -> float:
    """Approximate one-sample KS p-value against Uniform(0,1). See src/simplification/README.md for details."""
    x = torch.sort(samples).values
    n = x.numel()
    if n == 0:
        return 1.0
    cdf = torch.arange(1, n + 1, dtype=torch.float32) / n
    d_plus = torch.max(cdf - x).item()
    d_minus = torch.max(x - (torch.arange(0, n, dtype=torch.float32) / n)).item()
    d = max(d_plus, d_minus)
    lam = (math.sqrt(n) + 0.12 + 0.11 / math.sqrt(n)) * d
    p = 2.0 * sum(((-1) ** (k - 1)) * math.exp(-2.0 * (k ** 2) * (lam ** 2)) for k in range(1, 8))
    return max(0.0, min(1.0, p))


def test_topk_no_positional_bias() -> None:
    """Assert retained positions look approximately uniform under constant scores. See src/simplification/README.md for details."""
    lengths = [200, 220, 240, 260, 280]
    boundaries = []
    offset = 0
    for n in lengths:
        boundaries.append((offset, offset + n))
        offset += n

    scores = torch.ones((offset,), dtype=torch.float32)
    retained = simplify_with_scores(scores, boundaries, compression_ratio=0.2)

    positions = []
    for s, e in boundaries:
        idx = torch.where(retained[s:e])[0].float()
        positions.append(idx / max(1, (e - s - 1)))
    norm_pos = torch.cat(positions)

    assert _ks_pvalue_uniform01(norm_pos) > 0.01
