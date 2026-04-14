"""Importance score visualization utilities. See src/visualization/README.md."""

from __future__ import annotations

import math
from typing import List, Optional

import matplotlib
matplotlib.use("Agg")  # headless rendering — must come before pyplot import

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import torch
from torch import Tensor


def plot_importance(
    points: Tensor,
    importance_scores: Tensor,
    title: str = "Point Importance",
    save_path: Optional[str] = None,
) -> None:
    """Scatter plot of trajectory points coloured by importance score."""
    lons   = points[:, 2].numpy()
    lats   = points[:, 1].numpy()
    scores = importance_scores.numpy()

    fig, ax = plt.subplots(figsize=(10, 7))
    sc = ax.scatter(lons, lats, c=scores, cmap="plasma", s=8, alpha=0.8, vmin=0.0, vmax=1.0)
    plt.colorbar(sc, ax=ax, label="Importance Score")
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(title)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)


def plot_simplification_results(
    trajectories: List[Tensor],
    all_points: Tensor,
    retained_mask: Tensor,
    importance_scores: Tensor,
    queries: Tensor,
    title: str = "AIS Trajectory Simplification Results",
    save_path: Optional[str] = None,
) -> None:
    """Visualise retained/removed points alongside query regions."""
    fig, ax = plt.subplots(figsize=(12, 8))

    # 1. Draw trajectory lines as thin gray context lines
    for traj in trajectories:
        lats = traj[:, 1].numpy()
        lons = traj[:, 2].numpy()
        ax.plot(lons, lats, color="lightgray", linewidth=0.5)

    # 2. Separate retained and removed points
    retained_points = all_points[retained_mask]
    removed_mask = ~retained_mask

    spatial_matches = (
        (all_points[:, None, 1] >= queries[None, :, 0]) &
        (all_points[:, None, 1] <= queries[None, :, 1]) &
        (all_points[:, None, 2] >= queries[None, :, 2]) &
        (all_points[:, None, 2] <= queries[None, :, 3])
    )
    spatiotemporal_matches = (
        spatial_matches &
        (all_points[:, None, 0] >= queries[None, :, 4]) &
        (all_points[:, None, 0] <= queries[None, :, 5])
    )
    in_spatiotemporal_query = spatiotemporal_matches.any(dim=1)

    removed_relevant_mask = removed_mask & in_spatiotemporal_query
    removed_irrelevant_mask = removed_mask & ~in_spatiotemporal_query

    removed_relevant_points = all_points[removed_relevant_mask]
    removed_irrelevant_points = all_points[removed_irrelevant_mask]

    # 3. Plot removed points outside any full spatiotemporal query
    if removed_irrelevant_points.shape[0] > 0:
        ax.scatter(
            removed_irrelevant_points[:, 2].numpy(),
            removed_irrelevant_points[:, 1].numpy(),
            s=5,
            color="red",
            alpha=0.25,
            label="Removed (outside query-time)",
            zorder=3,
        )

    # 3b. Plot removed points that are query-relevant in full spatiotemporal terms
    if removed_relevant_points.shape[0] > 0:
        ax.scatter(
            removed_relevant_points[:, 2].numpy(),
            removed_relevant_points[:, 1].numpy(),
            s=12,
            color="orange",
            alpha=0.8,
            label="Removed (query-relevant)",
            zorder=5,
        )

    # 4. Plot retained points coloured by importance score
    sc = ax.scatter(
        retained_points[:, 2].numpy(),
        retained_points[:, 1].numpy(),
        c=importance_scores[retained_mask].numpy(),
        cmap="viridis",
        s=8,
        label="Retained Points",
        zorder=4,
    )
    plt.colorbar(sc, ax=ax, label="Importance Score")

    # 4b. Overlay retained start/end points with distinctive markers
    if all_points.shape[1] >= 7:
        retained_start_mask = retained_mask & (all_points[:, 5] > 0.5)
        retained_end_mask   = retained_mask & (all_points[:, 6] > 0.5)
        if retained_start_mask.any():
            ax.scatter(
                all_points[retained_start_mask, 2].numpy(),
                all_points[retained_start_mask, 1].numpy(),
                s=50, color="green", marker="o", zorder=7,
                label="Trajectory start",
            )
        if retained_end_mask.any():
            ax.scatter(
                all_points[retained_end_mask, 2].numpy(),
                all_points[retained_end_mask, 1].numpy(),
                s=50, color="blue", marker="s", zorder=7,
                label="Trajectory end",
            )

    # 5. Draw query rectangles
    query_patch = None
    for q in queries:
        lat_min, lat_max = float(q[0]), float(q[1])
        lon_min, lon_max = float(q[2]), float(q[3])
        rect = mpatches.Rectangle(
            (lon_min, lat_min),
            lon_max - lon_min,
            lat_max - lat_min,
            linewidth=1,
            edgecolor="blue",
            facecolor="blue",
            alpha=0.15,
            zorder=2,
        )
        ax.add_patch(rect)
        if query_patch is None:
            query_patch = rect

    # 6. Legend
    handles, labels = ax.get_legend_handles_labels()
    if query_patch is not None:
        handles.append(mpatches.Patch(facecolor="blue", alpha=0.15, edgecolor="blue", label="Queries"))
        labels.append("Queries")
    ax.legend(handles=handles, labels=labels, fontsize=8, loc="best")

    # 7–8. Title and axis labels
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.tight_layout()

    # 9. Save or close
    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)


def plot_trajectories_with_importance_and_queries(
    trajectories: List[Tensor],
    points: Tensor,
    importance_scores: Tensor,
    queries: Tensor,
    title: str = "Trajectories: Importance + Queries",
    save_path: Optional[str] = None,
) -> None:
    """Combined plot: trajectory lines coloured by importance with query overlays."""
    fig, ax = plt.subplots(figsize=(12, 8))
    cmap_traj = plt.get_cmap("tab20")

    # Draw trajectory lines
    for i, traj in enumerate(trajectories):
        lats = traj[:, 1].numpy()
        lons = traj[:, 2].numpy()
        ax.plot(lons, lats, "-", color=cmap_traj(i % 20), linewidth=0.8, alpha=0.4)

    # Draw points coloured by importance
    lons   = points[:, 2].numpy()
    lats   = points[:, 1].numpy()
    scores = importance_scores.numpy()
    sc = ax.scatter(lons, lats, c=scores, cmap="plasma", s=10, alpha=0.9, vmin=0.0, vmax=1.0, zorder=3)
    plt.colorbar(sc, ax=ax, label="Importance Score")

    # Draw query rectangles
    for q in queries:
        lat_min, lat_max = float(q[0]), float(q[1])
        lon_min, lon_max = float(q[2]), float(q[3])
        rect = mpatches.Rectangle(
            (lon_min, lat_min), lon_max - lon_min, lat_max - lat_min,
            linewidth=0.8,
            edgecolor="cyan",
            facecolor="cyan",
            alpha=0.07,
            zorder=2,
        )
        ax.add_patch(rect)

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(title)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)


def plot_simplification_time_slices(
    all_points: Tensor,
    retained_mask: Tensor,
    importance_scores: Tensor,
    queries: Tensor,
    title: str = "AIS Simplification (Time-Sliced)",
    n_slices: int = 4,
    save_path: Optional[str] = None,
) -> None:
    """Visualise simplification results across n_slices temporal windows."""
    n_slices = max(1, int(n_slices))

    times = all_points[:, 0]
    t_min = float(times.min().item())
    t_max = float(times.max().item())

    n_cols = 2 if n_slices > 1 else 1
    n_rows = math.ceil(n_slices / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(7 * n_cols, 4.5 * n_rows), squeeze=False)
    axes_flat = axes.ravel()

    removed_mask = ~retained_mask

    spatial_matches = (
        (all_points[:, None, 1] >= queries[None, :, 0]) &
        (all_points[:, None, 1] <= queries[None, :, 1]) &
        (all_points[:, None, 2] >= queries[None, :, 2]) &
        (all_points[:, None, 2] <= queries[None, :, 3])
    )
    spatiotemporal_matches = (
        spatial_matches &
        (all_points[:, None, 0] >= queries[None, :, 4]) &
        (all_points[:, None, 0] <= queries[None, :, 5])
    )
    in_spatiotemporal_query = spatiotemporal_matches.any(dim=1)

    removed_relevant_mask = removed_mask & in_spatiotemporal_query
    removed_irrelevant_mask = removed_mask & ~in_spatiotemporal_query

    edges = torch.linspace(t_min, t_max, steps=n_slices + 1)
    retained_artist = None

    for i in range(n_slices):
        ax = axes_flat[i]
        t0 = float(edges[i].item())
        t1 = float(edges[i + 1].item())

        if i == n_slices - 1:
            in_slice = (times >= t0) & (times <= t1)
        else:
            in_slice = (times >= t0) & (times < t1)

        removed_irrelevant_slice = removed_irrelevant_mask & in_slice
        removed_relevant_slice = removed_relevant_mask & in_slice
        retained_slice = retained_mask & in_slice

        if removed_irrelevant_slice.any():
            ax.scatter(
                all_points[removed_irrelevant_slice, 2].numpy(),
                all_points[removed_irrelevant_slice, 1].numpy(),
                s=6,
                color="red",
                alpha=0.25,
                zorder=3,
            )

        if removed_relevant_slice.any():
            ax.scatter(
                all_points[removed_relevant_slice, 2].numpy(),
                all_points[removed_relevant_slice, 1].numpy(),
                s=14,
                color="orange",
                alpha=0.85,
                zorder=5,
            )

        if retained_slice.any():
            retained_artist = ax.scatter(
                all_points[retained_slice, 2].numpy(),
                all_points[retained_slice, 1].numpy(),
                c=importance_scores[retained_slice].numpy(),
                cmap="viridis",
                vmin=0.0,
                vmax=1.0,
                s=10,
                zorder=4,
            )

        active_queries = (
            (queries[:, 5] >= t0) &
            (queries[:, 4] <= t1)
        )

        for q in queries[active_queries]:
            lat_min, lat_max = float(q[0]), float(q[1])
            lon_min, lon_max = float(q[2]), float(q[3])
            rect = mpatches.Rectangle(
                (lon_min, lat_min),
                lon_max - lon_min,
                lat_max - lat_min,
                linewidth=0.8,
                edgecolor="blue",
                facecolor="blue",
                alpha=0.10,
                zorder=2,
            )
            ax.add_patch(rect)

        ax.set_title(f"t=[{t0:.0f}, {t1:.0f}]  active queries={int(active_queries.sum().item())}")
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    for i in range(n_slices, len(axes_flat)):
        axes_flat[i].axis("off")

    if retained_artist is not None:
        cax = fig.add_axes([0.915, 0.22, 0.015, 0.62])
        cbar = fig.colorbar(retained_artist, cax=cax)
        cbar.set_label("Importance Score")

    legend_handles = [
        mpatches.Patch(facecolor="blue", edgecolor="blue", alpha=0.10, label="Queries active in slice"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="red", alpha=0.25,
                   markersize=6, label="Removed (outside query-time)"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="orange", alpha=0.85,
                   markersize=7, label="Removed (query-relevant)"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="green", alpha=0.9,
                   markersize=6, label="Retained"),
    ]
    legend = fig.legend(
        handles=legend_handles,
        fontsize=8,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=2,
        frameon=True,
    )
    legend.get_frame().set_alpha(0.95)

    fig.suptitle(title)
    fig.subplots_adjust(left=0.06, right=0.89, bottom=0.18, top=0.90, wspace=0.22, hspace=0.30)

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)


def plot_turn_scores(
    points: Tensor,
    retained_mask: Tensor,
    title: str = "AIS Turn-Score Visualization",
    save_path: Optional[str] = None,
) -> None:
    """Scatter plot highlighting trajectory points by turn intensity (column 7)."""
    if points.shape[1] < 8:
        print(
            "plot_turn_scores: turn_score column (col 7) not present "
            f"(points has {points.shape[1]} features). Skipping."
        )
        return

    lons = points[:, 2].numpy()
    lats = points[:, 1].numpy()
    turn = points[:, 7].numpy()

    fig, ax = plt.subplots(figsize=(11, 7))

    # All points, coloured by turn score using a hot colormap.
    sc = ax.scatter(
        lons, lats,
        c=turn,
        cmap="hot_r",
        s=8,
        alpha=0.6,
        vmin=0.0,
        vmax=1.0,
        zorder=3,
        label="All points (turn intensity)",
    )
    plt.colorbar(sc, ax=ax, label="Turn Score (angle / π)")

    # Highlight retained points at high-turn locations.
    high_turn_threshold = 0.2
    high_turn_retained = retained_mask & (points[:, 7] > high_turn_threshold)
    if high_turn_retained.any():
        ax.scatter(
            points[high_turn_retained, 2].numpy(),
            points[high_turn_retained, 1].numpy(),
            s=60,
            marker="*",
            color="deepskyblue",
            edgecolors="black",
            linewidths=0.5,
            zorder=6,
            label=f"Retained high-turn (score > {high_turn_threshold:.1f})",
        )

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(title)
    ax.legend(fontsize=8, loc="best")
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)
