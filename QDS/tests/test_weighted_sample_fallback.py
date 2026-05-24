"""Regression tests for the weighted-sample fallback used when the candidate
set exceeds torch.multinomial's 2^24 cap.

This guards against the phase-2A failure where the combined-day training CSV
has more than 2^24 points and torch.multinomial errors out.
"""

from __future__ import annotations

import torch

from src.queries.query_generator import _weighted_sample_one, generate_typed_query_workload


def test_weighted_sample_one_small_uses_multinomial_path() -> None:
    """For small inputs (< 2^24), the helper still produces in-range, distribution-respecting samples."""
    weights = torch.tensor([1.0, 0.0, 3.0, 0.0, 6.0], dtype=torch.float32)
    g = torch.Generator().manual_seed(0)
    counts = [0] * weights.numel()
    for _ in range(2000):
        idx = _weighted_sample_one(weights, g)
        counts[idx] += 1
    # Indices 1 and 3 have weight 0 → must never be sampled.
    assert counts[1] == 0
    assert counts[3] == 0
    # Index 4 (weight 6) should be the most frequent; index 0 (weight 1) the least
    # among non-zero weights.
    assert counts[4] > counts[2] > counts[0] > 0


def test_weighted_sample_one_above_cap_falls_back() -> None:
    """For inputs > 2^24, _weighted_sample_one must succeed where torch.multinomial would error.

    We construct a synthetic weight vector slightly larger than 2^24 with a
    single high-weight slot. The fallback should consistently return that slot.
    """
    cap = 1 << 24
    n = cap + 100
    weights = torch.zeros((n,), dtype=torch.float32)
    weights[42] = 1.0  # only this index has positive weight
    g = torch.Generator().manual_seed(123)
    for _ in range(10):
        idx = _weighted_sample_one(weights, g)
        assert idx == 42, f"expected fallback to land on weighted slot, got {idx}"


def test_weighted_sample_one_zero_weights_uniform_fallback() -> None:
    """Zero-total weights should produce a uniform random index (not crash)."""
    weights = torch.zeros((10,), dtype=torch.float32)
    g = torch.Generator().manual_seed(0)
    seen: set[int] = set()
    for _ in range(50):
        idx = _weighted_sample_one(weights, g)
        assert 0 <= idx < 10
        seen.add(idx)
    # Should hit several different indices (uniform random over 50 trials).
    assert len(seen) > 1


def test_generate_typed_query_workload_small_dataset_still_works() -> None:
    """Phase-2A single-day path: small dataset must continue to work after the
    multinomial-fallback addition. Regenerate a tiny workload and assert it
    produces the right number of queries with sane coverage."""
    torch.manual_seed(0)
    trajectories = []
    for traj_idx in range(4):
        n = 30
        t = torch.arange(n, dtype=torch.float32)
        lat = 55.0 + 0.01 * torch.arange(n, dtype=torch.float32) + 0.1 * traj_idx
        lon = 10.0 + 0.01 * torch.arange(n, dtype=torch.float32) + 0.1 * traj_idx
        speed = torch.full((n,), 10.0)
        heading = torch.full((n,), 90.0)
        is_start = torch.zeros(n)
        is_start[0] = 1.0
        is_end = torch.zeros(n)
        is_end[-1] = 1.0
        turn = torch.zeros(n)
        traj = torch.stack([t, lat, lon, speed, heading, is_start, is_end, turn], dim=1)
        trajectories.append(traj)

    workload = generate_typed_query_workload(
        trajectories=trajectories,
        n_queries=8,
        workload_mix={"range": 0.5, "knn": 0.5},
        seed=42,
    )
    assert len(workload.typed_queries) == 8
    types = {q["type"] for q in workload.typed_queries}
    assert types <= {"range", "knn"}
    assert workload.coverage_fraction is None or 0.0 <= workload.coverage_fraction <= 1.0
