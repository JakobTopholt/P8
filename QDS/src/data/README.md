# Data Module

This module loads AIS trajectories into per-trajectory tensors and provides the flattened and boundary views used by training, simplification, and evaluation.

## Files

| File | Purpose |
| --- | --- |
| `ais_loader.py` | Load AIS CSV files or generate deterministic synthetic trajectories. |
| `trajectory_dataset.py` | Wrap a list of trajectory tensors and expose flattened points plus trajectory boundaries. |

## AIS Tensor Schema

Each trajectory tensor has 8 columns:

| Col | Meaning |
| --- | --- |
| 0 | time in seconds |
| 1 | latitude |
| 2 | longitude |
| 3 | speed |
| 4 | heading |
| 5 | `is_start` flag |
| 6 | `is_end` flag |
| 7 | `turn_score` (normalized absolute heading change) |

## Loader Behavior

- `load_ais_csv(csv_path, max_points_per_ship=None)` accepts common column aliases: `mmsi` / `ship_id` / `vessel_id`, `lat` / `latitude`, `lon` / `longitude`, `speed` / `sog`, `heading` / `cog`, and `timestamp` / `time` / `datetime`.
- Rows are grouped by vessel id, sorted by timestamp, and trajectories shorter than 4 points are dropped.
- If `max_points_per_ship` is set, long trajectories are downsampled with evenly spaced indices.
- `generate_synthetic_ais_data(n_ships=24, n_points_per_ship=200, seed=42)` produces deterministic pseudo-realistic trajectories when no CSV is supplied.

## Dataset Helpers

- `TrajectoryDataset.get_all_points()` concatenates all trajectories into one `[N, F]` tensor.
- `TrajectoryDataset.get_trajectory_boundaries()` returns `(start, end)` pairs for each trajectory in flattened order.
- Trajectory boundaries are the contract used by the model, training loop, simplification, and evaluation code to keep attention and metrics trajectory-local.
