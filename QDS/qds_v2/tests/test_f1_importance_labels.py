"""Tests F1-contribution training labels. See src/training/README.md for details."""

from __future__ import annotations

import pytest
import torch

from src.queries.query_types import (
    QUERY_TYPE_ID_CLUSTERING,
    QUERY_TYPE_ID_KNN,
    QUERY_TYPE_ID_RANGE,
    QUERY_TYPE_ID_SIMILARITY,
)
from src.training.importance_labels import compute_typed_importance_labels


def test_range_labels_match_singleton_point_f1_contribution() -> None:
    """Assert range labels equal the F1 gain of recovering one matching point."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 3.0],
            [1.0, 5.0, 5.0, 9.0],
            [0.0, 0.1, 0.1, 1.0],
            [1.0, 6.0, 6.0, 7.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 2), (2, 4)]
    queries = [
        {
            "type": "range",
            "params": {
                "lat_min": -1.0,
                "lat_max": 1.0,
                "lon_min": -1.0,
                "lon_max": 1.0,
                "t_start": -1.0,
                "t_end": 0.5,
            },
        }
    ]

    labels, labelled_mask = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    expected_gain = 2.0 / 3.0
    assert labels[0, QUERY_TYPE_ID_RANGE].item() == pytest.approx(expected_gain)
    assert labels[2, QUERY_TYPE_ID_RANGE].item() == pytest.approx(expected_gain)
    assert labels[1, QUERY_TYPE_ID_RANGE].item() == pytest.approx(0.0)
    assert labels[3, QUERY_TYPE_ID_RANGE].item() == pytest.approx(0.0)
    assert bool(labelled_mask[:, QUERY_TYPE_ID_RANGE].all().item())


def test_range_labels_reward_each_in_box_point() -> None:
    """Assert duplicate in-box points are scored as individual range-query hits."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],
            [1.0, 0.1, 0.1, 1.0],
            [2.0, 5.0, 5.0, 1.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 3)]
    queries = [
        {
            "type": "range",
            "params": {
                "lat_min": -1.0,
                "lat_max": 1.0,
                "lon_min": -1.0,
                "lon_max": 1.0,
                "t_start": -1.0,
                "t_end": 1.5,
            },
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    expected_gain = 2.0 / 3.0
    assert labels[0, QUERY_TYPE_ID_RANGE].item() == pytest.approx(expected_gain)
    assert labels[1, QUERY_TYPE_ID_RANGE].item() == pytest.approx(expected_gain)
    assert labels[2, QUERY_TYPE_ID_RANGE].item() == pytest.approx(0.0)


def test_knn_labels_follow_f1_trajectory_hits_not_speed() -> None:
    """Assert kNN labels are driven by F1 hit membership instead of speed heuristics."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],
            [10.0, 4.0, 4.0, 100.0],
            [0.0, 2.0, 2.0, 200.0],
            [10.0, 8.0, 8.0, 300.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 2), (2, 4)]
    queries = [
        {
            "type": "knn",
            "params": {
                "lat": 0.0,
                "lon": 0.0,
                "t_center": 0.0,
                "t_half_window": 0.5,
                "k": 1,
            },
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    assert labels[0, QUERY_TYPE_ID_KNN].item() == pytest.approx(1.0)
    assert labels[1, QUERY_TYPE_ID_KNN].item() == pytest.approx(0.0)
    assert labels[2, QUERY_TYPE_ID_KNN].item() == pytest.approx(0.0)
    assert labels[3, QUERY_TYPE_ID_KNN].item() == pytest.approx(0.0)


def test_knn_labels_all_inwindow_points_when_below_representative_cap() -> None:
    """Assert small selected trajectories keep dense kNN supervision."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],    # traj 0, point 0 — inside window
            [0.1, 0.0, 0.001, 1.0],  # traj 0, point 1 — inside window
            [0.2, 0.0, 0.002, 1.0],  # traj 0, point 2 — inside window
            [0.3, 20.0, 20.0, 1.0],  # traj 0, point 3 — inside window (far, but still in)
            [0.0, 5.0, 5.0, 1.0],    # traj 1         — NOT selected by k=1
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 4), (4, 5)]
    queries = [
        {
            "type": "knn",
            "params": {
                "lat": 0.0,
                "lon": 0.0,
                "t_center": 0.0,
                "t_half_window": 1.0,
                "k": 1,
            },
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    # All 4 in-window points of trajectory 0 receive positive label because they are below the cap.
    assert labels[:4, QUERY_TYPE_ID_KNN].sum().item() > 0.0
    # Trajectory 1 is not selected (k=1 picks closest trajectory, which is traj 0)
    assert labels[4, QUERY_TYPE_ID_KNN].item() == pytest.approx(0.0)
    assert labels[:4, QUERY_TYPE_ID_KNN].sum().item() == pytest.approx(1.0)


def test_range_label_variant_uniform_drops_boundary_proximity_bias() -> None:
    """Assert range uniform variant gives every in-box point equal label.

    Two trajectories cross a single range box. Trajectory 0 enters and exits the box
    (so legacy boundary-detection assigns 2x boost to its first/last in-box points).
    Trajectory 1 is far away — irrelevant. The uniform variant should give all
    in-box points the same per-point label, while legacy gives the boundary-points a
    higher value. Total per-query label mass differs between variants because the
    legacy version is mean-1-normalized within trajectory and the uniform version
    skips that normalization, but the *spread* across in-box points must be flat for
    uniform.
    """
    points = torch.tensor(
        [
            # trajectory 0: enters box at point 0, in box for 4 points, exits at 4
            [0.0, 0.0, 0.0, 1.0],   # before box
            [1.0, 0.5, 0.5, 1.0],   # IN box (first in-box point — boundary)
            [2.0, 0.6, 0.6, 1.0],   # IN box (interior)
            [3.0, 0.7, 0.7, 1.0],   # IN box (interior)
            [4.0, 0.8, 0.8, 1.0],   # IN box (last in-box point — boundary)
            [5.0, 5.0, 5.0, 1.0],   # after box
            # trajectory 1: far away, never in box
            [0.0, 10.0, 10.0, 1.0],
            [5.0, 10.5, 10.5, 1.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 6), (6, 8)]
    queries = [
        {
            "type": "range",
            "params": {
                "lat_min": 0.4, "lat_max": 0.9,
                "lon_min": 0.4, "lon_max": 0.9,
                "t_start": 0.5, "t_end": 4.5,
            },
        }
    ]

    legacy_labels, _ = compute_typed_importance_labels(
        points, boundaries, queries, seed=1, range_label_variant="legacy"
    )
    uni_labels, _ = compute_typed_importance_labels(
        points, boundaries, queries, seed=1, range_label_variant="uniform"
    )

    in_box_idx = [1, 2, 3, 4]
    legacy_in_box = legacy_labels[in_box_idx, QUERY_TYPE_ID_RANGE].tolist()
    uni_in_box = uni_labels[in_box_idx, QUERY_TYPE_ID_RANGE].tolist()

    # Uniform variant: all in-box points get the SAME label.
    assert max(uni_in_box) == pytest.approx(min(uni_in_box), abs=1e-6)

    # Legacy variant: boundary points (first and last in-box points) get a strictly
    # higher label than interior in-box points (boundary boost = 2x).
    assert legacy_in_box[0] > legacy_in_box[1]
    assert legacy_in_box[3] > legacy_in_box[2]

    # Out-of-box points stay at zero in both variants.
    out_box_idx = [0, 5, 6, 7]
    assert all(legacy_labels[i, QUERY_TYPE_ID_RANGE].item() == 0.0 for i in out_box_idx)
    assert all(uni_labels[i, QUERY_TYPE_ID_RANGE].item() == 0.0 for i in out_box_idx)


def test_knn_label_variant_distance_weighted_concentrates_on_closest() -> None:
    """Assert distance_weighted kNN labels concentrate label mass on the closest in-window point.

    With four in-window points at increasing distance from the anchor, the closest
    representative should receive the largest label, and the per-trajectory label
    sum should still equal the trajectory's gain (here 1.0 with one query, k=1, one
    answer trajectory).
    """
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],     # closest  → biggest label
            [0.1, 0.0, 0.10, 1.0],    # 2nd
            [0.2, 0.0, 0.20, 1.0],    # 3rd
            [0.3, 0.0, 0.30, 1.0],    # 4th
            [0.0, 5.0, 5.0, 1.0],     # other trajectory, not selected
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 4), (4, 5)]
    queries = [
        {
            "type": "knn",
            "params": {
                "lat": 0.0,
                "lon": 0.0,
                "t_center": 0.0,
                "t_half_window": 1.0,
                "k": 1,
            },
        }
    ]

    legacy_labels, _ = compute_typed_importance_labels(
        points, boundaries, queries, seed=1, knn_label_variant="legacy"
    )
    dw_labels, _ = compute_typed_importance_labels(
        points, boundaries, queries, seed=1, knn_label_variant="distance_weighted"
    )

    # Sums match across the trajectory's representatives — both variants distribute
    # the same total gain.
    assert legacy_labels[:4, QUERY_TYPE_ID_KNN].sum().item() == pytest.approx(
        dw_labels[:4, QUERY_TYPE_ID_KNN].sum().item(), abs=1e-5
    )
    # Legacy splits gain equally (all four equal, monotonic-equal).
    legacy_vals = legacy_labels[:4, QUERY_TYPE_ID_KNN].tolist()
    assert max(legacy_vals) == pytest.approx(min(legacy_vals), abs=1e-5)
    # Distance-weighted is strictly decreasing as distance grows.
    dw_vals = dw_labels[:4, QUERY_TYPE_ID_KNN].tolist()
    assert dw_vals[0] > dw_vals[1] > dw_vals[2] > dw_vals[3]


def test_knn_labels_keep_nearest_high_cap_representatives() -> None:
    """Assert kNN labels cap very dense trajectories at the nearest 64 representatives."""
    near_points = [[float(i), 0.0, float(i) * 0.001, 1.0] for i in range(64)]
    far_points = [[float(64 + i), 10.0, 10.0 + float(i) * 0.001, 1.0] for i in range(6)]
    points = torch.tensor(near_points + far_points, dtype=torch.float32)
    boundaries = [(0, len(points))]
    queries = [
        {
            "type": "knn",
            "params": {
                "lat": 0.0,
                "lon": 0.0,
                "t_center": 35.0,
                "t_half_window": 100.0,
                "k": 1,
            },
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    positives = labels[:, QUERY_TYPE_ID_KNN] > 0.0
    assert int(positives.sum().item()) == 64
    assert bool(positives[:64].all().item())
    assert not bool(positives[64:].any().item())
    assert labels[:, QUERY_TYPE_ID_KNN].sum().item() == pytest.approx(1.0)


def test_similarity_labels_keep_reference_nearest_representatives() -> None:
    """Assert dense similarity labels focus on points nearest the reference snippet."""
    near_points = [[float(i), 0.0, float(i) * 0.001, 1.0] for i in range(64)]
    far_points = [[float(64 + i), 10.0, 10.0 + float(i) * 0.001, 1.0] for i in range(6)]
    points = torch.tensor(near_points + far_points, dtype=torch.float32)
    boundaries = [(0, len(points))]
    queries = [
        {
            "type": "similarity",
            "params": {
                "lat_query_centroid": 0.0,
                "lon_query_centroid": 0.0,
                "t_start": -1.0,
                "t_end": 100.0,
                "radius": 100.0,
                "top_k": 1,
            },
            "reference": points[:5, :3].tolist(),
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    positives = labels[:, QUERY_TYPE_ID_SIMILARITY] > 0.0
    assert int(positives.sum().item()) == 64
    assert bool(positives[:64].all().item())
    assert not bool(positives[64:].any().item())
    assert labels[:, QUERY_TYPE_ID_SIMILARITY].sum().item() == pytest.approx(1.0)


def test_knn_labels_exclude_out_of_window_points() -> None:
    """Assert kNN labels exclude points of the selected trajectory that are outside the time window."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],   # traj 0, inside window  [t_center=0 ± 0.5]
            [0.3, 0.0, 0.0, 1.0],   # traj 0, inside window
            [5.0, 0.0, 0.0, 1.0],   # traj 0, OUTSIDE window
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 3)]
    queries = [
        {
            "type": "knn",
            "params": {
                "lat": 0.0,
                "lon": 0.0,
                "t_center": 0.0,
                "t_half_window": 0.5,
                "k": 1,
            },
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    # The two in-window points are labeled; the out-of-window point is not
    assert labels[0, QUERY_TYPE_ID_KNN].item() > 0.0
    assert labels[1, QUERY_TYPE_ID_KNN].item() > 0.0
    assert labels[2, QUERY_TYPE_ID_KNN].item() == pytest.approx(0.0)


def test_clustering_labels_use_pairwise_f1_membership() -> None:
    """Assert clustering labels reward trajectories that participate in true co-membership pairs."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0, 1.0],
            [0.0, 0.01, 0.0, 1.0],
            [0.0, 0.02, 0.0, 1.0],
            [0.0, 5.0, 5.0, 1.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 1), (1, 2), (2, 3), (3, 4)]
    queries = [
        {
            "type": "clustering",
            "params": {
                "lat_min": -1.0,
                "lat_max": 6.0,
                "lon_min": -1.0,
                "lon_max": 6.0,
                "t_start": -1.0,
                "t_end": 1.0,
                "eps": 0.05,
                "min_samples": 2,
            },
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    assert labels[0, QUERY_TYPE_ID_CLUSTERING].item() > 0.0
    assert labels[1, QUERY_TYPE_ID_CLUSTERING].item() > 0.0
    assert labels[2, QUERY_TYPE_ID_CLUSTERING].item() > 0.0
    assert labels[3, QUERY_TYPE_ID_CLUSTERING].item() == pytest.approx(0.0)


def test_similarity_labels_follow_executor_centroid_geometry() -> None:
    """Assert similarity labels support trajectories selected by execution geometry."""
    points = torch.tensor(
        [
            [0.0, -1.0, 0.0, 1.0],
            [1.0, 1.0, 0.0, 1.0],
            [0.0, 5.0, 5.0, 1.0],
            [1.0, 6.0, 5.0, 1.0],
        ],
        dtype=torch.float32,
    )
    boundaries = [(0, 2), (2, 4)]
    queries = [
        {
            "type": "similarity",
            "params": {
                "lat_query_centroid": 0.0,
                "lon_query_centroid": 0.0,
                "t_start": -1.0,
                "t_end": 2.0,
                "radius": 0.25,
                "top_k": 1,
            },
            "reference": [[0.0, -1.0, 0.0], [1.0, 1.0, 0.0]],
        }
    ]

    labels, _ = compute_typed_importance_labels(points, boundaries, queries, seed=1)

    assert labels[0, QUERY_TYPE_ID_SIMILARITY].item() == pytest.approx(0.5)
    assert labels[1, QUERY_TYPE_ID_SIMILARITY].item() == pytest.approx(0.5)
    assert labels[2, QUERY_TYPE_ID_SIMILARITY].item() == pytest.approx(0.0)
    assert labels[3, QUERY_TYPE_ID_SIMILARITY].item() == pytest.approx(0.0)