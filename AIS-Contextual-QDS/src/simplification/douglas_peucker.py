"""Douglas-Peucker simplification baseline with target-point control."""

from __future__ import annotations

import math


def _perpendicular_distance(px: float, py: float, x1: float, y1: float, x2: float, y2: float) -> float:
    """Perpendicular distance from point p to line segment (x1,y1)->(x2,y2)."""
    dx = x2 - x1
    dy = y2 - y1
    norm = math.hypot(dx, dy)
    if norm == 0:
        return math.hypot(px - x1, py - y1)

    cross = abs(dx * (y1 - py) - (x1 - px) * dy)
    return cross / norm


def _dp_indices(points: list[tuple[float, float]], epsilon: float) -> list[int]:
    """Iterative Douglas-Peucker returning kept point indices."""
    n_points = len(points)
    if n_points <= 2:
        return list(range(n_points))

    keep = [False] * n_points
    keep[0] = True
    keep[-1] = True

    stack: list[tuple[int, int]] = [(0, n_points - 1)]
    while stack:
        start_idx, end_idx = stack.pop()
        if end_idx - start_idx <= 1:
            continue

        x1, y1 = points[start_idx]
        x2, y2 = points[end_idx]

        max_dist = -1.0
        split_idx = -1
        for idx in range(start_idx + 1, end_idx):
            px, py = points[idx]
            dist = _perpendicular_distance(px, py, x1, y1, x2, y2)
            if dist > max_dist:
                max_dist = dist
                split_idx = idx

        if split_idx >= 0 and max_dist > epsilon:
            keep[split_idx] = True
            stack.append((start_idx, split_idx))
            stack.append((split_idx, end_idx))

    return [idx for idx, flag in enumerate(keep) if flag]


def _resample_to_target(indices: list[int], n_points: int, target_points: int) -> list[int]:
    """Adjust candidate indices to exact target while preserving endpoints."""
    if n_points <= 2 or target_points >= n_points:
        return list(range(n_points))

    target_points = max(2, target_points)
    base = sorted(set(indices))
    if 0 not in base:
        base.insert(0, 0)
    if n_points - 1 not in base:
        base.append(n_points - 1)

    if len(base) == target_points:
        return base

    if len(base) > target_points:
        interior = base[1:-1]
        need = target_points - 2
        if need <= 0:
            return [0, n_points - 1]

        step = len(interior) / need
        selected: list[int] = []
        used = set()
        for rank in range(need):
            pos = int(round(rank * step))
            pos = min(max(pos, 0), len(interior) - 1)
            while pos in used and pos < len(interior) - 1:
                pos += 1
            used.add(pos)
            selected.append(interior[pos])
        return [0] + sorted(selected) + [n_points - 1]

    missing = target_points - len(base)
    pool = [idx for idx in range(1, n_points - 1) if idx not in set(base)]
    if not pool:
        return base

    if missing >= len(pool):
        return sorted(set(base + pool))

    step = len(pool) / (missing + 1)
    additions: list[int] = []
    for rank in range(1, missing + 1):
        pos = int(round(rank * step)) - 1
        pos = min(max(pos, 0), len(pool) - 1)
        additions.append(pool[pos])

    return sorted(set(base + additions))


def simplify_douglas_peucker_indices(
    points: list[tuple[float, float]],
    target_points: int,
    *,
    search_iterations: int = 24,
) -> list[int]:
    """Return approximately target-sized Douglas-Peucker simplification indices."""
    n_points = len(points)
    if n_points <= 2 or target_points >= n_points:
        return list(range(n_points))

    target_points = max(2, target_points)

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    bbox_diag = math.hypot(max(xs) - min(xs), max(ys) - min(ys))
    high = max(bbox_diag, 1e-9)
    low = 0.0

    best = _dp_indices(points, low)
    best_gap = abs(len(best) - target_points)

    for _ in range(max(1, search_iterations)):
        eps = (low + high) / 2.0
        candidate = _dp_indices(points, eps)
        gap = abs(len(candidate) - target_points)
        if gap < best_gap:
            best = candidate
            best_gap = gap

        if len(candidate) > target_points:
            low = eps
        else:
            high = eps

    return _resample_to_target(best, n_points, target_points)
