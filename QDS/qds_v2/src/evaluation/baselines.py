"""Baseline simplification methods for AIS-QDS v2 evaluation. See src/evaluation/README.md for details."""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
from typing import Protocol

import numpy as np
import torch

from src.experiments.experiment_config import TypedQueryWorkload
from src.simplification.simplify_trajectories import (
    evenly_spaced_indices,
    simplify_with_score_and_coverage,
    simplify_with_scores,
    simplify_with_temporal_score_hybrid,
)
from src.training.train_model import TrainingOutputs
from src.training.training_pipeline import windowed_predict


class Method(Protocol):
    """Simplification method protocol. See src/evaluation/README.md for details."""

    name: str

    def simplify(
        self,
        points: torch.Tensor,
        boundaries: list[tuple[int, int]],
        compression_ratio: float,
    ) -> torch.Tensor:
        """Return retained point mask. See src/evaluation/README.md for details."""


@dataclass
class MLQDSMethod:
    """Query-aware model-based simplification method. See src/evaluation/README.md for details."""

    name: str
    trained: TrainingOutputs
    workload: TypedQueryWorkload
    workload_mix: dict[str, float]
    temporal_fraction: float = 0.75
    diversity_bonus: float = 0.05
    simplification_mode: str = "score_coverage"
    coverage_lambda: float = 0.5
    coverage_sigma_fraction: float = 0.5

    def simplify(self, points: torch.Tensor, boundaries: list[tuple[int, int]], compression_ratio: float) -> torch.Tensor:
        """Simplify using workload-weighted typed scores.

        If the workload contains no queries, keep every point because there is
        no query F1 objective to optimize. Otherwise, use the learned workload-
        weighted score with a temporal-coverage base, then fill the remaining
        budget with learned query-aware scores.

        See ``src/evaluation/README.md`` for details.
        """
        if not self.workload.typed_queries:
            return torch.ones((points.shape[0],), dtype=torch.bool, device=points.device)

        point_dim = self.trained.model.point_dim
        p, q = self.trained.scaler.transform(points[:, :point_dim], self.workload.query_features)
        pred = windowed_predict(
            model=self.trained.model,
            norm_points=p,
            boundaries=boundaries,
            queries=q,
            query_type_ids=self.workload.type_ids,
        )

        type_order = ["range", "knn", "similarity", "clustering"]
        mix = torch.tensor([float(self.workload_mix.get(t, 0.0)) for t in type_order], dtype=torch.float32)
        if float(mix.sum().item()) <= 0.0:
            mix = torch.ones_like(mix) / mix.numel()
        else:
            mix = mix / mix.sum()
        # Rank-normalize per head within each trajectory before mixing so
        # head magnitudes (e.g. range head saturating high while sim head stays
        # low) don't dominate the workload-weighted sum.
        score = pred.new_zeros((pred.shape[0],))
        n_types = pred.shape[1]
        for s, e in boundaries:
            length = e - s
            if length <= 0:
                continue
            denom = float(max(1, length - 1))
            for t_idx in range(n_types):
                w = float(mix[t_idx].item())
                if w <= 0.0:
                    continue
                head = pred[s:e, t_idx]
                ranks = head.argsort().argsort().to(torch.float32) / denom
                score[s:e] = score[s:e] + w * ranks
        mode = str(self.simplification_mode).lower()
        if mode == "score_coverage":
            return simplify_with_score_and_coverage(
                score,
                boundaries,
                compression_ratio,
                coverage_lambda=self.coverage_lambda,
                coverage_sigma_fraction=self.coverage_sigma_fraction,
            )
        if mode == "topk":
            return simplify_with_scores(score, boundaries, compression_ratio)
        return simplify_with_temporal_score_hybrid(
            score,
            boundaries,
            compression_ratio,
            temporal_fraction=self.temporal_fraction,
            diversity_bonus=self.diversity_bonus,
        )


@dataclass
class RandomMethod:
    """Random baseline method. See src/evaluation/README.md for details."""

    name: str = "Random"
    seed: int = 0

    def simplify(self, points: torch.Tensor, boundaries: list[tuple[int, int]], compression_ratio: float) -> torch.Tensor:
        """Retain random points per trajectory at matched ratio. See src/evaluation/README.md for details."""
        g = torch.Generator().manual_seed(int(self.seed))
        scores = torch.rand((points.shape[0],), generator=g)
        return simplify_with_scores(scores, boundaries, compression_ratio)


@dataclass
class UniformTemporalMethod:
    """Uniform temporal sampling baseline. See src/evaluation/README.md for details."""

    name: str = "UniformTemporal"

    def simplify(self, points: torch.Tensor, boundaries: list[tuple[int, int]], compression_ratio: float) -> torch.Tensor:
        """Retain approximately every k-th point per trajectory. See src/evaluation/README.md for details."""
        scores = torch.zeros((points.shape[0],), dtype=torch.float32)
        for start, end in boundaries:
            n = end - start
            idx = torch.arange(n, dtype=torch.float32)
            scores[start:end] = -torch.abs(idx - (n - 1) / 2.0)
        return simplify_with_scores(scores, boundaries, compression_ratio)


@dataclass
class NewUniformTemporalMethod:
    """True evenly spaced temporal sampling baseline."""

    name: str = "uniform"

    def simplify(self, points: torch.Tensor, boundaries: list[tuple[int, int]], compression_ratio: float) -> torch.Tensor:
        """Retain evenly spaced points per trajectory, including endpoints."""
        retained = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
        for start, end in boundaries:
            n = end - start
            if n <= 0:
                continue
            k = max(2, int(torch.ceil(torch.tensor(float(compression_ratio) * n)).item()))
            k = min(k, n)
            local_idx = evenly_spaced_indices(n, k, points.device)
            retained[start + local_idx] = True
        return retained


@dataclass
class DouglasPeuckerMethod:
    """Recursive Douglas-Peucker geometric baseline (heap-based).

    Standard DP recursion: keep first/last point, split at the inner point with
    max perpendicular distance to the chord. Implemented as a heap so we only
    perform the K splits needed to hit the target compression — not the full
    log(N) recursion depth — which is critical for long AIS trajectories.

    Returns a per-trajectory point retained mask directly; no surrogate scores.
    """

    name: str = "DouglasPeucker"

    @staticmethod
    def _farthest_in_segment(xy: np.ndarray, s: int, e: int) -> tuple[int, float]:
        """Return (split_idx, max_perp) for points strictly between s and e."""
        if e - s < 2:
            return -1, 0.0
        a = xy[s]
        b = xy[e]
        v = b - a
        v_norm_sq = float(v[0] * v[0] + v[1] * v[1])
        seg = xy[s + 1 : e]
        if v_norm_sq < 1e-12:
            d = np.linalg.norm(seg - a, axis=1)
        else:
            rel = seg - a
            proj = (rel @ v) / v_norm_sq
            closest = a + proj[:, None] * v
            d = np.linalg.norm(seg - closest, axis=1)
        local = int(np.argmax(d))
        return s + 1 + local, float(d[local])

    def _dp_retained_mask(self, traj_xy_np: np.ndarray, k_keep: int) -> np.ndarray:
        """Retain the k_keep points produced by DP recursion (largest perp first)."""
        n = int(traj_xy_np.shape[0])
        mask = np.zeros((n,), dtype=bool)
        if n == 0 or k_keep <= 0:
            return mask
        mask[0] = True
        if n == 1 or k_keep == 1:
            return mask
        mask[n - 1] = True
        if k_keep <= 2:
            return mask

        # Negative-distance heap so largest perp pops first.
        heap: list[tuple[float, int, int, int]] = []
        split_idx, perp = self._farthest_in_segment(traj_xy_np, 0, n - 1)
        if split_idx >= 0:
            heapq.heappush(heap, (-perp, split_idx, 0, n - 1))

        kept = 2
        tie = 0  # heap-stability tiebreak counter (keeps push order deterministic)
        while heap and kept < k_keep:
            _, split_idx, s, e = heapq.heappop(heap)
            if mask[split_idx]:
                continue
            mask[split_idx] = True
            kept += 1
            left_split, left_perp = self._farthest_in_segment(traj_xy_np, s, split_idx)
            if left_split >= 0:
                tie += 1
                heapq.heappush(heap, (-left_perp, left_split, s, split_idx))
            right_split, right_perp = self._farthest_in_segment(traj_xy_np, split_idx, e)
            if right_split >= 0:
                tie += 1
                heapq.heappush(heap, (-right_perp, right_split, split_idx, e))
        return mask

    def simplify(self, points: torch.Tensor, boundaries: list[tuple[int, int]], compression_ratio: float) -> torch.Tensor:
        """Retain DP-selected points per trajectory at the requested ratio."""
        retained = torch.zeros((points.shape[0],), dtype=torch.bool, device=points.device)
        ratio = max(0.0, min(1.0, float(compression_ratio)))
        xy_np = points[:, 1:3].detach().cpu().numpy().astype(np.float64)
        for start, end in boundaries:
            n = int(end - start)
            if n <= 0:
                continue
            k = max(2, int(math.ceil(ratio * n)))
            k = min(k, n)
            traj_mask = self._dp_retained_mask(xy_np[start:end], k)
            retained[start:end] = torch.from_numpy(traj_mask).to(retained.device)
        return retained


@dataclass
class OracleMethod:
    """Diagnostic upper-bound method using oracle labels directly. See src/evaluation/README.md for details."""

    labels: torch.Tensor
    workload_mix: dict[str, float]
    name: str = "Oracle"

    def simplify(self, points: torch.Tensor, boundaries: list[tuple[int, int]], compression_ratio: float) -> torch.Tensor:
        """Simplify using weighted oracle labels. See src/evaluation/README.md for details."""
        mix = torch.tensor(
            [
                self.workload_mix.get("range", 0.0),
                self.workload_mix.get("knn", 0.0),
                self.workload_mix.get("similarity", 0.0),
                self.workload_mix.get("clustering", 0.0),
            ],
            dtype=torch.float32,
        )
        mix = mix / max(float(mix.sum().item()), 1e-6)
        score = (self.labels * mix.unsqueeze(0)).sum(dim=1)
        return simplify_with_scores(score, boundaries, compression_ratio)
