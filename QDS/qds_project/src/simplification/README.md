# Simplification Module

Compresses AIS trajectory data by removing low-importance points as predicted
by a trained QDS model.

---

## Component

### `simplify_trajectories.py`

**`simplify_trajectories(points, model, queries, threshold, compression_ratio, ...)`**  
Simplifies the full point cloud using model-predicted importance scores.
Returns a tuple of `(simplified_points, retained_mask, importance_scores)`.

When the model cannot score the full point cloud at once, points are processed
in chunks using the same normalization statistics as the full pass.

---

## Simplification Modes

### Per-Trajectory Compression (default)

Selected when `compression_ratio` is not `None` (default: `0.2`).

Each trajectory is compressed independently. A per-trajectory point budget is
computed as:

```
points_to_keep = max(min_points_per_trajectory,
                     int(compression_ratio * trajectory_length))
```

The top-`points_to_keep` points by importance score are selected. Trajectory
endpoints (first and last point) are guaranteed to be retained by temporarily
boosting their scores to `+inf` before top-k selection. This ensures no
trajectory is fully removed and compression is distributed evenly.

### Global Threshold Mode (legacy)

Selected when `compression_ratio=None`.

Points whose predicted importance score falls below `threshold` are discarded.
Trajectory-aware endpoint-retention and minimum-point-floor constraints are
applied afterwards.

---

## Score Selection

The function compares model scores against query-driven importance scores to
detect degenerate model outputs:

- If the model score span is `< 1e-6` (all scores nearly identical), the
  query-driven scores are used as fallback.
- If the top-1% of model-scored points do not
  have above-average query importance, query-driven scores are used.
- Otherwise, model scores are used.

If `model_max_points` is exceeded, chunked scoring is used instead of full-batch
inference so large trajectories remain supported without changing the output.

---

## Turn-Score Bias

When `turn_bias_weight > 0` and `points` has an 8th column (`turn_score`),
a small additive bias is applied before ranking:

```
final_score = predicted_importance + turn_bias_weight * turn_score
```

This nudges the ranker toward retaining points at bends while keeping
endpoints and query-relevant points dominant. Recommended value for the
turn-aware model is `0.1`.

---

## Trajectory Boundary Inference

When `trajectory_boundaries` is not provided, boundaries are inferred from
the `is_start` flag (column 5). Each point where `is_start == 1.0` begins a
new trajectory.

---

## Parameters

| Parameter                  | Default | Description                                                                    |
|----------------------------|---------|--------------------------------------------------------------------------------|
| `points`                   | —       | `[N, 7]` or `[N, 8]` point cloud tensor                                         |
| `model`                    | —       | Trained `TrajectoryQDSModel` or `TurnAwareQDSModel`                             |
| `queries`                  | —       | `[M, 6]` query workload tensor                                                 |
| `threshold`                | 0.5     | Score threshold for global threshold mode                                      |
| `compression_ratio`        | 0.2     | Per-trajectory fraction to retain; `None` → threshold                          |
| `min_points_per_trajectory`| 3       | Minimum retained points per trajectory                                         |
| `turn_bias_weight`         | 0.0     | Additive weight for `turn_score` bias                                          |
| `model_max_points`         | 300000  | Full-batch inference cap; above this, model scoring runs in chunks             |
| `importance_chunk_size`    | 200000  | Chunk size for query score computation                                         |
| `trajectory_boundaries`    | None    | Override inferred boundaries                                                   |
