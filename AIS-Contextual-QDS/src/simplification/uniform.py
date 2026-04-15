"""Uniform temporal simplification baseline."""

from __future__ import annotations


def simplify_uniform_indices(n_points: int, target_points: int) -> list[int]:
    """Return uniformly distributed indices while preserving endpoints."""
    if n_points <= 0:
        return []

    if n_points <= 2 or target_points >= n_points:
        return list(range(n_points))

    target_points = max(2, target_points)
    interior_needed = target_points - 2
    interior_count = n_points - 2

    if interior_needed <= 0:
        return [0, n_points - 1]

    if interior_needed >= interior_count:
        return list(range(n_points))

    step = interior_count / (interior_needed + 1)
    chosen: list[int] = []
    used = set()
    for rank in range(1, interior_needed + 1):
        idx = int(round(rank * step))
        idx = min(max(idx, 1), n_points - 2)
        if idx in used:
            idx += 1
            while idx in used and idx <= n_points - 2:
                idx += 1
            if idx > n_points - 2:
                idx = max(i for i in range(1, n_points - 1) if i not in used)
        used.add(idx)
        chosen.append(idx)

    return [0] + sorted(chosen) + [n_points - 1]
