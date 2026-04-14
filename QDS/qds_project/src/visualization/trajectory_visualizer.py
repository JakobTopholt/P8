"""Trajectory visualization utilities for AIS data. See src/visualization/README.md."""

from __future__ import annotations

from typing import List, Optional

import matplotlib
matplotlib.use("Agg")  # headless rendering — must come before pyplot import

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import torch
from torch import Tensor


def plot_trajectories(
    trajectories: List[Tensor],
    title: str = "AIS Trajectories",
    save_path: Optional[str] = None,
) -> None:
    """Plot vessel trajectories as coloured lines in lat/lon space."""
    fig, ax = plt.subplots(figsize=(10, 7))
    cmap = plt.get_cmap("tab20")

    for i, traj in enumerate(trajectories):
        lats = traj[:, 1].numpy()
        lons = traj[:, 2].numpy()
        color = cmap(i % 20)
        ax.plot(lons, lats, "-", color=color, linewidth=1.0, alpha=0.8, label=f"Ship {i}")
        ax.plot(lons[0],  lats[0],  "o", color="green", markersize=6, zorder=5)
        ax.plot(lons[-1], lats[-1], "s", color="blue",  markersize=6, zorder=5)

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(title)
    if len(trajectories) <= 10:
        ax.legend(fontsize=7, loc="best")

    # Endpoint legend entries
    start_handle = plt.Line2D(
        [0], [0], marker="o", color="w", markerfacecolor="green", markersize=7,
    )
    end_handle = plt.Line2D(
        [0], [0], marker="s", color="w", markerfacecolor="blue", markersize=7,
    )
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles=handles + [start_handle, end_handle],
        labels=labels + ["Start point", "End point"],
        fontsize=7, loc="best",
    )
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)


def plot_queries_on_trajectories(
    trajectories: List[Tensor],
    queries: Tensor,
    title: str = "Trajectories with Queries",
    save_path: Optional[str] = None,
) -> None:
    """Overlay semi-transparent query rectangles on trajectory paths."""
    fig, ax = plt.subplots(figsize=(10, 7))
    cmap = plt.get_cmap("tab20")

    # Draw trajectories
    for i, traj in enumerate(trajectories):
        lats = traj[:, 1].numpy()
        lons = traj[:, 2].numpy()
        ax.plot(lons, lats, "-", color=cmap(i % 20), linewidth=1.0, alpha=0.7)

    # Draw query rectangles (lon on x-axis, lat on y-axis)
    for q in queries:
        lat_min, lat_max = float(q[0]), float(q[1])
        lon_min, lon_max = float(q[2]), float(q[3])
        width  = lon_max - lon_min
        height = lat_max - lat_min
        rect = mpatches.Rectangle(
            (lon_min, lat_min), width, height,
            linewidth=0.8,
            edgecolor="red",
            facecolor="red",
            alpha=0.05,
        )
        ax.add_patch(rect)

    # Legend entry for query rectangles
    query_handle = mpatches.Patch(
        facecolor="red", edgecolor="red", alpha=0.4,
        label=f"Range query ({queries.shape[0]})",
    )
    ax.legend(handles=[query_handle], fontsize=7, loc="best")

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(title)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)


# Colours for each query type
_QUERY_TYPE_COLORS = {
    "range": "red",
    "intersection": "royalblue",
    "aggregation": "green",
    "nearest": "darkorange",
}

# Edge line styles per query type for box-type queries
_QUERY_TYPE_LINESTYLES = {
    "range": "-",
    "intersection": "--",
    "aggregation": ":",
}


def plot_typed_queries_on_trajectories(
    trajectories: List[Tensor],
    typed_queries: list,
    title: str = "Trajectories with Typed Queries",
    save_path: Optional[str] = None,
) -> None:
    """Overlay typed queries on trajectory paths with type-specific rendering.

    Rendering conventions
    ---------------------
    * ``range`` queries — solid red semi-transparent rectangles.
    * ``intersection`` queries — dashed blue semi-transparent rectangles.
    * ``aggregation`` queries — dotted green semi-transparent rectangles.
    * ``nearest`` / kNN queries — orange markers at the query location.
      The marker shape indicates *k*: a circle (``o``) for *k=1* and a star
      (``*``) for *k>1*.  Marker size scales with ``sqrt(k)`` and a small
      text annotation shows the *k* value.

    A legend distinguishes query types and shows per-type query counts.

    Args:
        trajectories: List of per-ship point tensors [T, F].
        typed_queries: List of typed query dicts (``"type"`` + ``"params"``).
        title: Plot title.
        save_path: Optional path at which to save the figure.
    """
    fig, ax = plt.subplots(figsize=(10, 7))
    cmap = plt.get_cmap("tab20")

    # Draw trajectories
    for i, traj in enumerate(trajectories):
        lats = traj[:, 1].numpy()
        lons = traj[:, 2].numpy()
        ax.plot(lons, lats, "-", color=cmap(i % 20), linewidth=1.0, alpha=0.7)

    # Collect per-type counts for the legend
    type_counts: dict[str, int] = {}
    # Track distinct k values seen in nearest queries
    knn_k_values: set[int] = set()

    for q in typed_queries:
        qtype = q.get("type", "range")
        params = q.get("params", {})
        color = _QUERY_TYPE_COLORS.get(qtype, "purple")
        type_counts[qtype] = type_counts.get(qtype, 0) + 1

        if qtype in ("range", "intersection", "aggregation"):
            lat_min = params.get("lat_min", 0.0)
            lat_max = params.get("lat_max", 0.0)
            lon_min = params.get("lon_min", 0.0)
            lon_max = params.get("lon_max", 0.0)
            width = lon_max - lon_min
            height = lat_max - lat_min
            linestyle = _QUERY_TYPE_LINESTYLES.get(qtype, "-")
            rect = mpatches.Rectangle(
                (lon_min, lat_min),
                width,
                height,
                linewidth=1.2,
                linestyle=linestyle,
                edgecolor=color,
                facecolor=color,
                alpha=0.06,
            )
            ax.add_patch(rect)
        elif qtype == "nearest":
            qlat = params.get("query_lat", 0.0)
            qlon = params.get("query_lon", 0.0)
            k = int(params.get("k", 1))
            knn_k_values.add(k)
            # Marker: circle for k=1, star for k>1; size scales with sqrt(k)
            marker = "o" if k == 1 else "*"
            marker_size = 18 + 6 * (k ** 0.5)
            ax.scatter(
                [qlon], [qlat],
                c=color,
                s=marker_size,
                marker=marker,
                zorder=5,
                alpha=0.8,
                linewidths=0,
            )
            # Annotate k value for kNN (k > 1) queries
            if k > 1:
                ax.annotate(
                    f"k={k}",
                    xy=(qlon, qlat),
                    fontsize=5,
                    color=color,
                    ha="left",
                    va="bottom",
                    alpha=0.8,
                )

    # Build legend: box-type entries use a Patch, nearest uses a Line2D
    legend_handles = []
    for qt in sorted(type_counts):
        count = type_counts[qt]
        color = _QUERY_TYPE_COLORS.get(qt, "purple")
        if qt in ("range", "intersection", "aggregation"):
            ls = _QUERY_TYPE_LINESTYLES.get(qt, "-")
            handle = mpatches.FancyBboxPatch(
                (0, 0), 1, 1,
                linewidth=1.5,
                linestyle=ls,
                edgecolor=color,
                facecolor=color,
                alpha=0.4,
                label=f"{qt.capitalize()} ({count})",
                boxstyle="square,pad=0",
            )
        elif qt == "nearest":
            k_label = ""
            if knn_k_values and max(knn_k_values) > 1:
                k_vals_str = "/".join(str(k_val) for k_val in sorted(knn_k_values))
                k_label = f", k={k_vals_str}"
            handle = plt.Line2D(
                [0], [0],
                marker="*",
                color="w",
                markerfacecolor=color,
                markersize=10,
                label=f"Nearest-kNN ({count}{k_label})",
            )
        else:
            handle = mpatches.Patch(facecolor=color, edgecolor=color, alpha=0.5,
                                    label=f"{qt.capitalize()} ({count})")
        legend_handles.append(handle)

    if legend_handles:
        ax.legend(handles=legend_handles, fontsize=7, loc="best",
                  framealpha=0.8, edgecolor="gray")

    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.set_title(title)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
    plt.close(fig)
