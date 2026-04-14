"""QDS model training pipeline. See src/training/README.md for details and CLI usage."""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Optional

import torch
import torch.nn as nn
from torch import Tensor

# Allow execution both as a module and directly
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from src.models.trajectory_qds_model import TrajectoryQDSModel
from src.models.trajectory_qds_model import normalize_points_and_queries
from src.models.turn_aware_qds_model import TurnAwareQDSModel
from src.training.importance_labels import compute_importance


def train_model(
    trajectories: List[Tensor],
    queries: Tensor,
    epochs: int = 50,
    lr: float = 1e-3,
    save_path: Optional[str] = None,
    importance: Optional[Tensor] = None,
    max_points: Optional[int] = None,
    importance_chunk_size: int = 200_000,
    point_batch_size: Optional[int] = 50_000,
    model_type: str = "baseline",
) -> TrajectoryQDSModel | TurnAwareQDSModel:
    """Train a TrajectoryQDSModel or TurnAwareQDSModel on AIS trajectory data."""
    # --- Flatten all trajectories into a single point cloud ---
    points = torch.cat(trajectories, dim=0)  # [N, 7] or [N, 8]

    # Keep preprocess tensors colocated before normalization.
    if queries.device != points.device:
        queries = queries.to(points.device)

    # --- Compute ground-truth importance labels ---
    if importance is None:
        print("Computing ground-truth importance labels …")
        importance = compute_importance(points, queries, chunk_size=importance_chunk_size)
    elif importance.shape[0] != points.shape[0]:
        raise ValueError("importance must have the same length as flattened points")

    if importance.device != points.device:
        importance = importance.to(points.device)

    # Sample before feature construction to avoid unnecessary preprocessing.
    sampled_points = points
    train_importance = importance
    if max_points is not None and sampled_points.shape[0] > max_points:
        sample_count = int(max_points)
        sample_idx = torch.randperm(sampled_points.shape[0], device=sampled_points.device)[:sample_count]
        sampled_points = sampled_points[sample_idx]
        train_importance = train_importance[sample_idx]
        print(
            f"Training on sampled subset: {sample_count}/{points.shape[0]} points "
            "(full dataset still used for evaluation/simplification)."
        )

    # Select feature slice for the chosen model type.
    # Baseline model uses 7 features; turn-aware uses 8.
    # If points have fewer features than required, zero-pad as needed.
    if model_type == "turn_aware":
        if sampled_points.shape[1] >= 8:
            train_points = sampled_points
        else:
            pad = torch.zeros(
                sampled_points.shape[0],
                8 - sampled_points.shape[1],
                device=sampled_points.device,
            )
            train_points = torch.cat([sampled_points, pad], dim=1)
    else:
        train_points = sampled_points[:, :7]

    # --- Normalise model inputs for stable training dynamics ---
    norm_points, norm_queries = normalize_points_and_queries(train_points, queries)

    # --- Select compute device (GPU if available, else CPU) ---
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # --- Move training tensors to device once before the training loop ---
    norm_points = norm_points.to(device)
    norm_queries = norm_queries.to(device)
    train_importance = train_importance.to(device)

    # --- Build model and optimizer ---
    if model_type == "turn_aware":
        model = TurnAwareQDSModel()
        print("Training TurnAwareQDSModel for", epochs, "epochs …")
    else:
        model = TrajectoryQDSModel()
        print(f"Training TrajectoryQDSModel for {epochs} epochs …")
    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    high_importance_weight = 9.0
    n_train = norm_points.shape[0]
    effective_batch = None if point_batch_size is None else max(1, int(point_batch_size))

    # --- Training loop ---
    model.train()
    for epoch in range(1, epochs + 1):
        if effective_batch is None or n_train <= effective_batch:
            optimizer.zero_grad()
            predicted = model(norm_points, norm_queries)
            weights = 1.0 + high_importance_weight * train_importance
            loss = ((predicted - train_importance) ** 2 * weights).mean()
            loss.backward()
            optimizer.step()
            epoch_loss = loss.item()
        else:
            perm = torch.randperm(n_train, device=norm_points.device)
            loss_sum = 0.0
            seen = 0

            for start in range(0, n_train, effective_batch):
                end = min(n_train, start + effective_batch)
                idx = perm[start:end]

                batch_points = norm_points[idx]
                batch_targets = train_importance[idx]

                optimizer.zero_grad()
                predicted = model(batch_points, norm_queries)
                weights = 1.0 + high_importance_weight * batch_targets
                batch_loss = ((predicted - batch_targets) ** 2 * weights).mean()
                batch_loss.backward()
                optimizer.step()

                batch_size = end - start
                loss_sum += batch_loss.item() * batch_size
                seen += batch_size

            epoch_loss = loss_sum / max(1, seen)

        if epoch % max(1, epochs // 10) == 0 or epoch == 1:
            print(f"  Epoch {epoch:4d}/{epochs}  loss={epoch_loss:.6f}")

    model.eval()

    # --- Optionally save model weights ---
    if save_path is not None:
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        torch.save(model.state_dict(), save_path)
        print(f"Model saved to: {save_path}")

    return model


def main() -> None:
    """Command-line entry point for training the TrajectoryQDSModel."""
    parser = argparse.ArgumentParser(
        description="Train the TrajectoryQDSModel on synthetic AIS data"
    )
    parser.add_argument("--n_ships",   type=int,   default=10,    help="Number of synthetic ships")
    parser.add_argument("--n_points",  type=int,   default=100,   help="Points per ship")
    parser.add_argument("--n_queries", type=int,   default=100,   help="Number of queries")
    parser.add_argument("--epochs",    type=int,   default=50,    help="Training epochs")
    parser.add_argument("--lr",        type=float, default=1e-3,  help="Learning rate")
    parser.add_argument(
        "--max_points",
        type=int,
        default=None,
        help="Optional max number of points used for training (random sample)",
    )
    parser.add_argument(
        "--point_batch_size",
        type=int,
        default=50000,
        help="Mini-batch size over points during training",
    )
    parser.add_argument(
        "--save_path",
        type=str,
        default=None,
        help="Path to save model weights (default: models/saved_models/trajectory_qds_model.pt)",
    )
    args = parser.parse_args()

    from src.data.ais_loader import generate_synthetic_ais_data
    from src.queries.query_generator import generate_spatiotemporal_queries

    trajectories = generate_synthetic_ais_data(
        n_ships=args.n_ships, n_points_per_ship=args.n_points
    )
    queries = generate_spatiotemporal_queries(trajectories, n_queries=args.n_queries)

    # Default save path relative to this file's location
    save_path = args.save_path
    if save_path is None:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        save_path = os.path.join(
            module_dir, "..", "models", "saved_models", "trajectory_qds_model.pt"
        )

    train_model(
        trajectories=trajectories,
        queries=queries,
        epochs=args.epochs,
        lr=args.lr,
        save_path=save_path,
        max_points=args.max_points,
        point_batch_size=args.point_batch_size,
    )


if __name__ == "__main__":
    main()
