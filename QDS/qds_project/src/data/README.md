# Data Module

Handles loading AIS trajectory data from CSV files and provides a synthetic
data generator for testing.

---

## Components

### `ais_loader.py`

**`load_ais_csv(filepath, turn_score_method)`**  
Loads AIS trajectories from a CSV file. Supports common schema variants
including AISDK-style fields (`Latitude`, `Longitude`, `SOG`). Groups rows by
MMSI, sorts each vessel's track by timestamp, and appends derived features.

Supported column aliases:
- `mmsi`
- `lat` or `latitude`
- `lon` or `longitude`
- `speed` or `sog`
- `heading` or `cog` (optional)
- `timestamp` / `time` / `datetime` (optional — synthesized if absent)

**`generate_synthetic_ais_data(n_ships, n_points_per_ship, save_path, turn_score_method)`**  
Generates synthetic AIS trajectory data. Ships move on straight-ish paths
with small random heading perturbations, placed in a North Sea–like region
(lat 50–60°N, lon 0–20°E).

#### Output Format

Both functions return a `List[Tensor]`, one tensor per vessel of shape
`[T, 8]` with columns:

| Index | Column       | Description                                    |
|-------|--------------|------------------------------------------------|
| 0     | `time`       | Unix timestamp (seconds)                       |
| 1     | `lat`        | Latitude (degrees)                             |
| 2     | `lon`        | Longitude (degrees)                            |
| 3     | `speed`      | Speed over ground (knots)                      |
| 4     | `heading`    | Course over ground (degrees)                   |
| 5     | `is_start`   | 1.0 for the first point of the trajectory      |
| 6     | `is_end`     | 1.0 for the last point of the trajectory       |
| 7     | `turn_score` | Normalised direction-change intensity in [0,1] |

#### Endpoint Flags

`is_start` and `is_end` are binary flags set to 1.0 for the first and last
points of each trajectory respectively. They allow the model to learn that
trajectory endpoints typically have higher structural importance.

#### Turn Score

`turn_score` stores a normalised direction-change intensity in [0, 1] for
each interior point. Endpoints receive 0.0. Two computation methods are
available:

- **`heading`** (default): uses wrapped heading/COG deltas — compares heading
  before and after each interior point using the shortest angular distance:
  ```
  delta = |((heading[i+1] - heading[i-1] + 180) % 360) - 180|
  turn_score[i] = delta / 180
  ```
- **`geometry`**: uses the angle between consecutive lat/lon displacement
  vectors — computes the angle between the incoming and outgoing trajectory
  segments at each interior point.

---

### `trajectory_dataset.py`

**`TrajectoryDataset`** — PyTorch `Dataset` wrapper for a list of trajectory
tensors.

| Method                       | Description                                            |
|------------------------------|--------------------------------------------------------|
| `__len__()`                  | Number of trajectories                                 |
| `__getitem__(idx)`           | Trajectory tensor at index `idx`                       |
| `get_all_points()`           | Flatten all trajectories into a single `[N, 8]` tensor |
| `get_trajectory_boundaries()`| `(start, end)` index pairs in the flattened point cloud|
