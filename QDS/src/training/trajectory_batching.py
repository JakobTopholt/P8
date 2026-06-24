"""Trajectory-window batching utilities for ranking training. See src/training/README.md for details."""

from __future__ import annotations

from dataclasses import dataclass

import torch


@dataclass
class TrajectoryBatch:
    """Padded trajectory window batch container. See src/training/README.md for details."""

    points: torch.Tensor
    padding_mask: torch.Tensor
    trajectory_ids: torch.Tensor
    global_indices: torch.Tensor


def build_trajectory_windows(
    points: torch.Tensor,
    boundaries: list[tuple[int, int]],
    window_length: int,
    stride: int,
    min_real_points: int | None = None,
) -> list[TrajectoryBatch]:
    """Build trajectory-local windows with no cross-trajectory attention. See src/training/README.md for details.

    min_real_points: skip windows whose real (non-padded) point count is below
    this threshold. Defaults to window_length // 16 (~6%), removing near-empty
    windows where padding dominates and training signal is minimal. Set to 0
    or 1 to retain all windows (e.g. for inference where every point needs a
    prediction).
    """
    if min_real_points is None:
        min_real_points = max(1, window_length // 16)
    windows: list[TrajectoryBatch] = []
    for tid, (start, end) in enumerate(boundaries):
        traj = points[start:end]
        n = traj.shape[0]
        if n <= window_length:
            if n < min_real_points:
                continue
            pad = window_length - n
            p = torch.cat([traj, torch.zeros((pad, traj.shape[1]), dtype=traj.dtype)], dim=0)
            mask = torch.zeros((window_length,), dtype=torch.bool)
            if pad > 0:
                mask[n:] = True
            idx = torch.cat([torch.arange(start, end), torch.full((pad,), -1, dtype=torch.long)])
            windows.append(
                TrajectoryBatch(
                    points=p.unsqueeze(0),
                    padding_mask=mask.unsqueeze(0),
                    trajectory_ids=torch.tensor([tid], dtype=torch.long),
                    global_indices=idx.unsqueeze(0),
                )
            )
            continue

        for w_start in range(0, n, stride):
            w_end = min(n, w_start + window_length)
            real_count = w_end - w_start
            if real_count < min_real_points:
                if w_end == n:
                    break
                continue
            chunk = traj[w_start:w_end]
            if chunk.shape[0] < window_length:
                pad = window_length - chunk.shape[0]
                chunk = torch.cat([chunk, torch.zeros((pad, traj.shape[1]), dtype=traj.dtype)], dim=0)
                mask = torch.zeros((window_length,), dtype=torch.bool)
                mask[window_length - pad :] = True
                idx = torch.cat(
                    [
                        torch.arange(start + w_start, start + w_end),
                        torch.full((pad,), -1, dtype=torch.long),
                    ]
                )
            else:
                mask = torch.zeros((window_length,), dtype=torch.bool)
                idx = torch.arange(start + w_start, start + w_end)
            windows.append(
                TrajectoryBatch(
                    points=chunk.unsqueeze(0),
                    padding_mask=mask.unsqueeze(0),
                    trajectory_ids=torch.tensor([tid], dtype=torch.long),
                    global_indices=idx.unsqueeze(0),
                )
            )
            if w_end == n:
                break
    return windows


def batch_windows(windows: list[TrajectoryBatch], batch_size: int) -> list[TrajectoryBatch]:
    """Group single-window TrajectoryBatches into mini-batches.

    Each input is shape (1, L, D) / (1, L); the grouped output has shape
    (B, L, D) / (B, L) where B = min(batch_size, remaining).  The forward pass
    of the model already supports arbitrary batch sizes, so grouping simply
    lets the GPU process several windows in a single call.
    """
    if batch_size <= 1 or not windows:
        return windows
    batched: list[TrajectoryBatch] = []
    for i in range(0, len(windows), batch_size):
        group = windows[i : i + batch_size]
        batched.append(
            TrajectoryBatch(
                points=torch.cat([w.points for w in group], dim=0),
                padding_mask=torch.cat([w.padding_mask for w in group], dim=0),
                trajectory_ids=torch.cat([w.trajectory_ids for w in group], dim=0),
                global_indices=torch.cat([w.global_indices for w in group], dim=0),
            )
        )
    return batched
